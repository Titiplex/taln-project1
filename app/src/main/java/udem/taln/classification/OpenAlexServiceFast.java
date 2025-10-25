package udem.taln.classification;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.io.IOException;
import java.net.URI;
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
import java.util.stream.Collectors;

public class OpenAlexServiceFast {

    private final HttpClient http;
    private final ObjectMapper M = new ObjectMapper();
    private final String mailto; // ex: "prenom.nom@domaine.tld"
    private final Semaphore concurrency; // limite de parallélisme
    private final Cache<String, OpenAlexService.Tuple> memCache; // DOI normalisé -> Tuple (ou null sentinel)
    private final Path diskCacheDir; // optionnel (peut être null)
    private final ScheduledExecutorService shed = Executors.newSingleThreadScheduledExecutor();
    private final AtomicInteger tokens = new AtomicInteger(0);
    private volatile int burstSize = 24;

    public OpenAlexServiceFast(String mailto, int maxParallel, int memCacheSize, Path diskCacheDir) {
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

    // ---------- API publique ----------

    /**
     * Lookup direct par DOI (synchrone convenable pour test).
     */
    public Optional<OpenAlexService.Tuple> getByDoi(String rawDoi) {
        String doi = normalizeDoi(rawDoi);
        if (doi == null) return Optional.empty();
        return Optional.ofNullable(loadCached(doi, () -> fetchByDoiBlocking(doi)));
    }

    /**
     * Batch: renvoie une map DOI normalisé -> Tuple (ou absent si introuvable)
     */
    public Map<String, OpenAlexService.Tuple> getByDoiBatch(Collection<String> rawDois) {
        List<String> dois = rawDois.stream()
                .map(OpenAlexServiceFast::normalizeDoi)
                .filter(Objects::nonNull)
                .distinct()
                .toList();

        Map<String, OpenAlexService.Tuple> out = new ConcurrentHashMap<>();
        List<String> misses = Collections.synchronizedList(new ArrayList<>());
        List<String> toFetch = new ArrayList<>();

        for (String doi : dois) {
            OpenAlexService.Tuple cached = loadCached(doi, null);
            if (cached != null) {
                out.put(doi, cached);
            } else {
                toFetch.add(doi);
            }
        }
        if (toFetch.isEmpty()) return out;

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String doi : toFetch) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    concurrency.acquire();
                    acquirePermit();
                    OpenAlexService.Tuple t = fetchByDoiBlocking(doi);
                    if (t != null) {
                        saveCached(doi, t);
                        out.put(doi, t);      // ✅ on NE met que des valeurs non nulles
                        System.out.println("Fetched for : " + doi + ", " + t);
                    } else {
                        misses.add(doi);      // pour inspection/relance éventuelle
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    concurrency.release();
                }
            }));
        }
        futures.forEach(CompletableFuture::join);

        if (!misses.isEmpty()) {
            System.err.println("[OpenAlex] misses=" + misses.size());
        }
        return out; // map ne contient que les DOI résolus
    }

    /**
     * +     * Ultra-rapide : batch par chunks (filter=doi:a|b|c) + fallback unitaire pour les ratés.
     * +     * @param rawDois DOIs (bruts)
     * +     * @param chunkSize ex: 50
     * +
     */
    public Map<String, OpenAlexService.Tuple> getByDoiBatchSmart(Collection<String> rawDois, int chunkSize) {
        // 1) normalise/dédupe
        List<String> dois = rawDois.stream()
                .map(OpenAlexServiceFast::normalizeDoi)
                .filter(Objects::nonNull).distinct().toList();
        System.out.println("Batch size: " + dois.size());
        // 2) cache
        Map<String, OpenAlexService.Tuple> out = new ConcurrentHashMap<>();
        List<String> toFetch = new ArrayList<>();
        for (String doi : dois) {
            OpenAlexService.Tuple cached = loadCached(doi, null);
            if (cached != null) out.put(doi, cached);
            else toFetch.add(doi);
        }
        if (toFetch.isEmpty()) return out;
        System.out.println("Fetching " + toFetch.size() + " DOIs");
        // 3) chunks
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < toFetch.size(); i += chunkSize) {
            chunks.add(toFetch.subList(i, Math.min(i + chunkSize, toFetch.size())));
        }
        // 4) paralléliser par chunk
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (List<String> chunk : chunks) {
            System.out.println("Chunk: " + chunk.size());
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    concurrency.acquire();
                    acquirePermit();
                    String filter = chunk.stream().collect(Collectors.joining("|", "doi:", ""));
                    String url = "https://api.openalex.org/works?filter=" + filter + "&per_page=200" + mailtoParam();
                    var req = HttpRequest.newBuilder(URI.create(url))
                            .timeout(Duration.ofSeconds(30))
                            .header("Accept", "application/json")
                            .header("Accept-Encoding", "gzip")
                            .GET().build();
                    var res = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
                    int sc = res.statusCode();
                    System.out.println("Chunk status: " + sc);
                    if (sc == 200) {
                        System.out.println("Fetched chunk: " + chunk.size());
                        // JSON streaming : on ne lit que doi, id, cited_by_count
                        var parser = M.getFactory().createParser(res.body());
                        boolean inResults = false;
                        String curDoi = null, curId = null;
                        Integer curCited = null;
                        while (!parser.isClosed()) {
                            var tok = parser.nextToken();
                            if (tok == null) break;
                            String name = parser.getCurrentName();
                            if (tok.isStructStart() && "results".equals(name)) {
                                inResults = true;
                                continue;
                            }
                            if (!inResults) continue;
                            if (tok.isStructStart()) {
                                curDoi = null;
                                curId = null;
                                curCited = null;
                            }
                            if (tok.isScalarValue()) {
                                if ("doi".equals(name)) curDoi = sanitizeDoi(parser.getValueAsString());
                                else if ("id".equals(name)) curId = trimOpenAlexId(parser.getValueAsString());
                                else if ("cited_by_count".equals(name)) curCited = parser.getIntValue();
                            }
                            if (tok.isStructEnd()) {
                                if (curDoi != null && curId != null) {
                                    var tuple = new OpenAlexService.Tuple(curId, curCited);
                                    saveCached(curDoi, tuple);
                                    out.put(curDoi, tuple);
                                    System.out.println("Fetched for : " + curDoi + ", " + tuple);
                                }
                            }
                        }
                    } else if (sc == 429 || sc >= 500) {
                        long wait = retryAfterMillis(res).orElse(800L);
                        System.err.println("Retry after " + wait + " ms");
                        sleepQuiet(wait);
                        // (optionnel) un retry simple ici si besoin
                    } else System.err.println("Unexpected status: " + sc);
                } catch (Exception ignored) {
                } finally {
                    concurrency.release();
                }
            }));
        }
        System.out.println("Futures: " + futures.size());
        futures.forEach(CompletableFuture::join);
        System.out.println("[Final] Fetched " + out.size() + " DOIs");

        // 5) fallback unitaire pour ce qui manque
        List<String> misses = toFetch.stream().filter(doi -> !out.containsKey(doi)).toList();
        if (!misses.isEmpty()) {
            out.putAll(getByDoiBatch(misses));
        }
        System.out.println("[Final] Fetched " + out.size() + " DOIs (with fallback)");
        return out;
    }

    // ---------- Implémentation ----------

    private OpenAlexService.Tuple fetchByDoiBlocking(String doi) {
        // disque -> réseau
        OpenAlexService.Tuple disk = readDiskCache(doi);
        if (disk != null) return disk;

        String url = "https://api.openalex.org/works/doi:" + doi + (mailtoParam());
        int tries = 0;
        long baseSleepMs = 200;
        while (tries++ < 6) {
            try {
                var req = HttpRequest.newBuilder(URI.create(url))
                        .timeout(Duration.ofSeconds(20))
                        .header("Accept", "application/json")
                        .header("Accept-Encoding", "gzip")
                        .GET().build();
                acquirePermit();
                var res = http.send(req, HttpResponse.BodyHandlers.ofString());
                int sc = res.statusCode();
                if (sc == 200) {
                    var node = M.readTree(res.body());
                    if (node == null || node.has("error")) return null;
                    String idUrl = asText(node.get("id"));
                    Integer cited = node.has("cited_by_count") ? node.get("cited_by_count").asInt() : null;
                    if (idUrl == null) return null;
                    String openAlexId = idUrl.substring(idUrl.lastIndexOf('/') + 1);
                    var tuple = new OpenAlexService.Tuple(openAlexId, cited);
                    writeDiskCache(doi, tuple);
                    return tuple;
                }
                if (sc == 429 || sc >= 500) {
                    // respecter Retry-After si présent
                    long wait = retryAfterMillis(res).orElse(backoffWithJitter(baseSleepMs, tries));
                    sleepQuiet(wait);
                    continue;
                }
                // 404 / autres -> pas de résultat
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

    private String mailtoParam() {
        return (mailto == null || mailto.isBlank()) ? "" : ("?mailto=" + mailto);
    }

    private static String asText(JsonNode n) {
        return (n == null || n.isNull()) ? null : n.asText();
    }

    private static String normalizeDoi(String raw) {
        if (raw == null) return null;
        String d = raw.trim().toLowerCase(Locale.ROOT);
        d = d.replaceFirst("^https?://(dx\\.)?doi\\.org/", "");
        if (d.isBlank()) return null;
        return java.net.URLEncoder.encode(d, StandardCharsets.UTF_8);
    }


    // ---------- Helpers batch ----------
    private static String trimOpenAlexId(String idUrl) {
        if (idUrl == null) return null;
        int idx = idUrl.lastIndexOf('/');
        return idx >= 0 ? idUrl.substring(idx + 1) : idUrl;
    }

    private static String sanitizeDoi(String doiField) {
        if (doiField == null) return null;
        String d = doiField.trim().toLowerCase(Locale.ROOT)
                .replaceFirst("^https?://(dx\\.)?doi\\.org/", "");
        return java.net.URLEncoder.encode(d, StandardCharsets.UTF_8);
    }

    // ---------- Rate limiter ----------

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
}