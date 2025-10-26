package udem.taln.classification;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class OpenAlexService {

    private final HttpClient http;
    private final ObjectMapper M = new ObjectMapper();
    private final String mailto;
    private final Semaphore concurrency;
    private final Cache<String, OpenAlexService.Tuple> memCache;
    private final Path diskCacheDir;
    private final ScheduledExecutorService shed = Executors.newSingleThreadScheduledExecutor();
    private final AtomicInteger tokens = new AtomicInteger(0);
    private volatile int burstSize = 24;

    public record Tuple(String openAlexId, Integer citedNumber) {
    }

    public OpenAlexService(String mailto, int maxParallel, int memCacheSize, Path diskCacheDir) {
        this.mailto = mailto;
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(10))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        this.concurrency = new Semaphore(Math.max(1, maxParallel));
        this.memCache = Caffeine.newBuilder()
                .maximumSize(memCacheSize)
                .expireAfterWrite(Duration.ofHours(12))
                .build();
        this.diskCacheDir = diskCacheDir;
        if (diskCacheDir != null) {
            try {
                Files.createDirectories(diskCacheDir);
            } catch (IOException ignored) {
            }
        }
        M.registerModule(new AfterburnerModule());
    }

    /**
     * +     * Ultra-rapide : batch par chunks (filter=doi:a|b|c) + fallback unitaire pour les ratés.
     * +     * @param rawDois DOIs (bruts)
     * +     * @param chunkSize ex: 50
     * +
     */
    public Map<String, OpenAlexService.Tuple> getByDoiBatchSmart(Collection<String> rawDois, int chunkSize) {
        List<String> dois = rawDois.stream()
                .map(OpenAlexService::toCanonicalDoi)
                .filter(Objects::nonNull).distinct().toList();

        Map<String, OpenAlexService.Tuple> out = new ConcurrentHashMap<>();
        List<String> toFetch = new ArrayList<>();
        for (String doi : dois) {
            OpenAlexService.Tuple cached = loadCached(doi, null);
            if (cached != null) out.put(doi, cached);
            else toFetch.add(doi);
        }
        if (toFetch.isEmpty()) return out;
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < toFetch.size(); i += chunkSize) {
            chunks.add(toFetch.subList(i, Math.min(i + chunkSize, toFetch.size())));
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (List<String> chunk : chunks) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    fetchChunkAdaptive(chunk, out);
                } catch (Exception t) {
                    System.err.println("[OpenAlex][chunk] FAILED size=" + chunk.size() + " : " + t);
                    t.printStackTrace(System.err);
                    throw new CompletionException(t);
                }
            }));
        }
        futures.forEach(CompletableFuture::join);

        List<String> misses = toFetch.stream().filter(doi -> !out.containsKey(doi)).toList();
        if (!misses.isEmpty()) {
            System.err.println("[OpenAlex] misses after batch=" + misses.size());
        }
        System.out.println("[Final] Fetched " + out.size() + " DOIs (with fallback)");
        return out;
    }

    private void fetchChunkAdaptive(List<String> chunk, Map<String, OpenAlexService.Tuple> out) throws Exception {
        if (estimationUrlLength(chunk) > 7500 && chunk.size() > 1) {
            int mid = chunk.size() / 2;
            fetchChunkAdaptive(chunk.subList(0, mid), out);
            fetchChunkAdaptive(chunk.subList(mid, chunk.size()), out);
            System.out.println("too long");
            return;
        }
        concurrency.acquire();
        try {
            acquirePermit();
            URI uri = buildWorksUri(chunk);
            System.out.println(" uri=" + uri);
            var req = HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofSeconds(30))
                    .header("Accept", "application/json")
                    .GET().build();
            var res = http.send(req, HttpResponse.BodyHandlers.ofString());
            int sc = res.statusCode();
            System.out.println(" sc=" + sc);
            if (sc == 200) {
                parseResultsFast(res.body(), out);
                return;
            }
            if (sc == 429) {
                sleepQuiet(retryAfterMillis(res).orElse(800L));
                fetchChunkAdaptive(chunk, out);
                return;
            }
            if ((sc == 414 || sc == 400 || sc == 413 || sc == 502 || sc == 503 || sc == 504) && chunk.size() > 1) {
                int mid = chunk.size() / 2;
                fetchChunkAdaptive(chunk.subList(0, mid), out);
                fetchChunkAdaptive(chunk.subList(mid, chunk.size()), out);
                return;
            }
            for (String doi : chunk) {
                var t = fetchByDoiBlocking(doi);
                if (t != null) {
                    saveCached(doi, t);
                    out.put(doi, t);
                }
            }
        } finally {
            concurrency.release();
        }
    }

    private void parseResultsFast(String body, Map<String, OpenAlexService.Tuple> out) throws IOException {
        JsonNode root = M.readTree(body);
        JsonNode results = root.get("results");
        if (results == null || !results.isArray()) return;

        int added = 0, skipped = 0;
        for (JsonNode r : results) {
            try {
                String doiField = asText(r.get("doi"));
                String idUrl = asText(r.get("id"));
                Integer cited = r.has("cited_by_count") && !r.get("cited_by_count").isNull()
                        ? r.get("cited_by_count").asInt() : null;
                if (doiField == null || idUrl == null) {
                    skipped++;
                    continue;
                }

                String canonical = canonicalFromReturnedDoi(doiField);
                if (canonical == null || canonical.isBlank()) {
                    skipped++;
                    continue;
                }

                String openAlexId = trimOpenAlexId(idUrl);
                if (openAlexId == null || openAlexId.isBlank()) {
                    skipped++;
                    continue;
                }

                var tuple = new OpenAlexService.Tuple(openAlexId, cited);

                saveCached(canonical, tuple);
                out.put(canonical, tuple);
                added++;
            } catch (Throwable t) {
                System.err.println("[OpenAlex][parse] skip one result: " + t);
            }
        }
        if (added == 0 && !results.isEmpty()) {
            System.err.println("[OpenAlex][parse] WARNING: results=" + results.size() + " but added=0 (skipped=" + skipped + ")");
            System.err.println(body.substring(0, Math.min(400, body.length())));
        }
    }

    private static int estimationUrlLength(List<String> dois) {
        int base = 30;
        int sum = dois.stream().mapToInt(s -> s.length() + 1).sum();
        return base + sum;
    }

    private OpenAlexService.Tuple fetchByDoiBlocking(String canonicalDoi) {
        OpenAlexService.Tuple disk = readDiskCache(canonicalDoi);
        if (disk != null) return disk;

        StringBuilder q = new StringBuilder("filter=doi:").append(canonicalDoi).append("&per-page=1");
        if (mailto != null && !mailto.isBlank()) {
            q.append("&mailto=").append(URLEncoder.encode(mailto, StandardCharsets.UTF_8));
        }
        URI uri = URI.create("https://api.openalex.org/works?" + q);

        int tries = 0;
        long baseSleepMs = 200;
        while (tries++ < 6) {
            try {
                var req = HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofSeconds(20))
                        .header("Accept", "application/json")
                        .GET().build();
                acquirePermit();
                var res = http.send(req, HttpResponse.BodyHandlers.ofString());
                int sc = res.statusCode();
                if (sc == 200) {
                    var root = M.readTree(res.body());
                    var results = root.get("results");
                    if (results != null && results.isArray() && !results.isEmpty()) {
                        var r = results.get(0);
                        String idUrl = asText(r.get("id"));
                        Integer cited = r.has("cited_by_count") && !r.get("cited_by_count").isNull()
                                ? r.get("cited_by_count").asInt() : null;
                        if (idUrl == null) return null;
                        String openAlexId = trimOpenAlexId(idUrl);
                        var tuple = new OpenAlexService.Tuple(openAlexId, cited);
                        writeDiskCache(canonicalDoi, tuple);
                        return tuple;
                    }
                    return null;
                }
                if (sc == 429 || sc >= 500) {
                    long wait = retryAfterMillis(res).orElse(backoffWithJitter(baseSleepMs, tries));
                    sleepQuiet(wait);
                    continue;
                }
                return null;
            } catch (Exception e) {
                sleepQuiet(backoffWithJitter(baseSleepMs, tries));
            }
        }
        return null;
    }

    private Optional<Long> retryAfterMillis(HttpResponse<?> res) {
        var ra = res.headers().firstValue("Retry-After");
        if (ra.isEmpty()) return Optional.empty();
        try {
            long s = Long.parseLong(ra.get());
            return Optional.of(Math.max(1000, s * 1000));
        } catch (NumberFormatException nfe) {
            return Optional.empty();
        }
    }

    private static long backoffWithJitter(long baseMs, int tries) {
        long pow = (long) Math.min(10_000, baseMs * Math.pow(2, tries - 1));
        long jitter = ThreadLocalRandom.current().nextLong(100, 400);
        return pow + jitter;
    }

    private static void sleepQuiet(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private OpenAlexService.Tuple loadCached(String doi, Callable<OpenAlexService.Tuple> loader) {
        OpenAlexService.Tuple t = memCache.getIfPresent(doi);
        if (t != null) return t;
        if (diskCacheDir != null) {
            OpenAlexService.Tuple d = readDiskCache(doi);
            if (d != null) {
                memCache.put(doi, d);
                return d;
            }
        }
        if (loader == null) return null;
        try {
            OpenAlexService.Tuple v = loader.call();
            if (v != null) saveCached(doi, v);
            return v;
        } catch (Exception e) {
            return null;
        }
    }

    private void saveCached(String doi, OpenAlexService.Tuple v) {
        memCache.put(doi, v);
        writeDiskCache(doi, v);
    }

    private OpenAlexService.Tuple readDiskCache(String doi) {
        if (diskCacheDir == null) return null;
        Path f = diskCacheDir.resolve(hash(doi) + ".json");
        if (!Files.exists(f)) return null;
        try {
            var node = M.readTree(Files.readString(f));
            String id = asText(node.get("openAlexId"));
            Integer c = node.has("citedNumber") && !node.get("citedNumber").isNull()
                    ? node.get("citedNumber").asInt() : null;
            return (id == null ? null : new OpenAlexService.Tuple(id, c));
        } catch (IOException e) {
            return null;
        }
    }

    private void writeDiskCache(String doi, OpenAlexService.Tuple v) {
        if (diskCacheDir == null || v == null) return;
        Path f = diskCacheDir.resolve(hash(doi) + ".json");
        try {
            Files.createDirectories(f.getParent());
            var node = Map.of("doi", doi, "openAlexId", v.openAlexId(), "citedNumber", v.citedNumber());
            Files.writeString(f, M.writeValueAsString(node), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException ignored) {
        }
    }

    private static String hash(String input) {
        final String algorithm = "SHA-256";
        try {
            byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
            byte[] digestBytes = java.security.MessageDigest.getInstance(algorithm).digest(inputBytes);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digestBytes);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("Missing hash algorithm: " + algorithm, e);
        }
    }

    private static String asText(JsonNode n) {
        return (n == null || n.isNull()) ? null : n.asText();
    }

    private static String trimOpenAlexId(String idUrl) {
        if (idUrl == null) return null;
        int idx = idUrl.lastIndexOf('/');
        return idx >= 0 ? idUrl.substring(idx + 1) : idUrl;
    }

    /**
     * Démarre un limiteur de débit simple (permits/sec, burst).
     */
    public void startRateLimiter(int permitsPerSecond, int burst) {
        this.burstSize = Math.max(permitsPerSecond, burst);
        tokens.set(burst);
        shed.scheduleAtFixedRate(() -> {
            int cur = tokens.get();
            int next = Math.min(burstSize, cur + permitsPerSecond);
            tokens.set(next);
        }, 0, 1, TimeUnit.SECONDS);
    }

    private void acquirePermit() throws InterruptedException {
        while (true) {
            int cur = tokens.get();
            if (cur > 0 && tokens.compareAndSet(cur, cur - 1)) return;
            Thread.sleep(8);
        }
    }

    static String toCanonicalDoi(String raw) {
        if (raw == null) return null;
        String s = raw.trim();
        try {
            s = java.net.URLDecoder.decode(s, java.nio.charset.StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ignore) {
        }
        String d = s.toLowerCase(java.util.Locale.ROOT)
                .replaceFirst("^https?://(dx\\.)?doi\\.org/", "")
                .replaceFirst("[\\s\\p{Punct}]+$", "");
        return d.isBlank() ? null : d;
    }

    private static String canonicalFromReturnedDoi(String returned) {
        return toCanonicalDoi(returned);
    }

    /**
     * +     * Construit une URI /works?filter=doi:VAL1%7CVAL2...
     * +     * - Chaque valeur est de la forme "<a href="https://doi.org/10.xxxx/yyy">...</a>" (NON encodée)
     * +     * - On encode UNIQUEMENT le séparateur '|' en %7C (ainsi que le mailto si présent)
     * +     * - Pas de double-encodage du paramètre 'filter'
     * +
     */
    private URI buildWorksUri(List<String> canonicalDois) {
        List<String> vals = canonicalDois.stream()
                .map(OpenAlexService::toCanonicalDoi)
                .filter(Objects::nonNull)
                .toList();
        if (vals.isEmpty()) throw new IllegalArgumentException("No valid DOIs to query.");

        String filterValue = "doi:" + String.join("%7C", vals);
        StringBuilder q = new StringBuilder();
        q.append("filter=").append(filterValue).append("&per-page=200");
        if (mailto != null && !mailto.isBlank()) {
            q.append("&mailto=").append(URLEncoder.encode(mailto, StandardCharsets.UTF_8));
        }

        String url = "https://api.openalex.org/works?" + q;
        return URI.create(url);
    }
}