package udem.taln.classification.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import udem.taln.wrapper.dto.PaperDto;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.*;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class Benchmark {
    public record BenchmarkRecord(
            String openAlexId,
            String doi,
            String title,
            int year,
            String venue,
            boolean isClassification,
            String benchmarkName,
            String domain,
            String labelSpace,
            String primaryMetric,
            Double bestScore,
            String bestModelName,
            Integer citedBy,
            List<String> referencedWorks,
            List<String> evidenceSnippets
    ) {
    }

    static final Map<String, String> DOMAIN = Map.ofEntries(
            Map.entry("sentiment", "sentiment"),
            Map.entry("polarity", "sentiment"),
            Map.entry("toxicity", "toxicity"),
            Map.entry("hate speech", "toxicity"),
            Map.entry("topic", "topic"),
            Map.entry("stance", "stance"),
            Map.entry("intent", "intent"),
            Map.entry("emotion", "emotion"),
            Map.entry("nli", "nli"),
            Map.entry("natural language inference", "nli"),
            Map.entry("dialect identification", "dialect-id"),
            Map.entry("language identification", "lang-id")
    );
    static final Pattern LABEL_BIN = Pattern.compile("\\bbinary\\b");
    static final Pattern LABEL_MULTI = Pattern.compile("\\bmulti(-|\\s)?class\\b");
    static final Pattern LABEL_MLABEL = Pattern.compile("\\bmulti(-|\\s)?label\\b");
    static final List<String> METRIC_PRIORITY = List.of(
            "macro-f1", "micro-f1", "f1", "accuracy", "mcc", "auroc", "auprc"
    );
    static final Pattern METRIC_NEAR_SCORE =
            Pattern.compile("(macro-?f1|micro-?f1|f1|accuracy|mcc|auroc|auprc)\\D{0,30}(\\d+(?:\\.\\d+)?)\\s?%?",
                    Pattern.CASE_INSENSITIVE);

    static final Pattern INTRO_BENCH = Pattern.compile(
            "(introduce|propose|present)\\s+(?:a\\s+)?(?:new\\s+)?(?:dataset|benchmark)\\s+([A-Za-z0-9][A-Za-z0-9_-]{2,})",
            Pattern.CASE_INSENSITIVE);

    public static Optional<String> detectBenchmarkName(String text) {
        if (text == null) return Optional.empty();
        var m = INTRO_BENCH.matcher(text); // use ORIGINAL text, not lowercased
        if (m.find()) return Optional.ofNullable(m.group(2));
        return Optional.empty();
    }


    public static Optional<String> detectDomain(String text) {
        String tl = text.toLowerCase();
        return DOMAIN.keySet().stream().filter(tl::contains).findFirst().map(DOMAIN::get);
    }

    public static String detectLabelSpace(String text) {
        String tl = text.toLowerCase();
        if (LABEL_MLABEL.matcher(tl).find()) return "multi-label";
        if (LABEL_MULTI.matcher(tl).find()) return "multi-class";
        if (LABEL_BIN.matcher(tl).find()) return "binary";
        return "other";
    }

    public static Optional<String> primaryMetric(String text) {
        String tl = text.toLowerCase();
        return METRIC_PRIORITY.stream().filter(tl::contains).findFirst();
    }

    public static Optional<Double> bestScore(String text, String metric) {
        if (metric == null) return Optional.empty();
        var m = METRIC_NEAR_SCORE.matcher(text);
        Double best = null;
        while (m.find()) {
            if (!m.group(1).equalsIgnoreCase(metric)) continue;
            double v = Double.parseDouble(m.group(2));
            best = (best == null || v > best) ? v : best;
        }
        return Optional.ofNullable(best);
    }

    public static List<String> evidence(String text, String... needled) {
        List<String> out = new ArrayList<>();
        for (String s : text.split("(?<=[.!?])\\s+")) {
            String sl = s.toLowerCase();
            boolean hit = false;
            for (String n : needled)
                if (n != null && !n.isBlank() && sl.contains(n.toLowerCase())) {
                    hit = true;
                    break;
                }
            if (hit) out.add(s.strip());
            if (out.size() >= 3) break;
        }
        return out;
    }

    public static BenchmarkRecord extractFromPaper(PaperDto p) {
        String text = tryFetchAnthologyText(p);
        if (text.isBlank() && p.pdfUrl() != null) {
            System.out.println("[Benchmark] Jsoup null, PDF URL: " + p.pdfUrl());
        }

        var benchName = detectBenchmarkName(text).orElse(null);
        var domain = detectDomain(text).orElse(null);
        var label = detectLabelSpace(text);
        var metric = primaryMetric(text).orElse(null);
        var score = bestScore(text, metric).orElse(null);
        var evid = evidence(text, benchName, domain, metric);

        return new BenchmarkRecord(
                p.openAlexId(), p.doi(), p.title(), p.year(), p.venue().name(),
                Boolean.TRUE.equals(p.isClassificationCandidate()),
                benchName, domain, label, metric, score, /*bestModelName*/ null,
                p.citedByCount(), p.referencedWorks(), evid
        );
    }

    static String tryFetchAnthologyText(PaperDto p) {
        // 1) Try to recover the ACL slug from pdfUrl or doi
        String slug = resolveAclSlug(p);
        String trim = ((p.title() == null ? "" : p.title()) + ". " + (p.abs() == null ? "" : p.abs())).trim();
        if (slug == null) {
            // Fall back: at least return title+abstract to keep detectors alive
            return trim;
        }

        String pageUrl = "https://aclanthology.org/" + slug + "/"; // HTML page
        try {
            Document d = Jsoup
                    .connect(pageUrl)
                    .userAgent("Mozilla/5.0 (compatible; TALN-crawler/1.0; +mailto:you@example.com)")
                    .referrer("https://aclanthology.org/")
                    .ignoreHttpErrors(true)
                    .followRedirects(true)
                    .maxBodySize(0)
                    .timeout(15_000)
                    .get();
            if (d != null && d.body() != null) return d.text();
            System.err.println("[Benchmark] Jsoup empty body, URL: " + pageUrl);
        } catch (Exception e) {
            System.err.println("[Benchmark] Jsoup error: " + e.getMessage() + ", URL=[" + pageUrl + "]");
        }

        // Last resort: title+abstract so extraction still returns something non-empty
        return trim;
    }

    private static String resolveAclSlug(PaperDto p) {
        // Prefer pdfUrl if present: https://aclanthology.org/2025.acl-long.1056.pdf
        if (p.pdfUrl() != null && p.pdfUrl().contains("aclanthology.org/")) {
            // extract "2025.acl-long.1056" between domain/ and .pdf
            String u = p.pdfUrl();
            int i = u.indexOf("aclanthology.org/");
            if (i >= 0) {
                String tail = u.substring(i + "aclanthology.org/".length());
                if (tail.endsWith(".pdf")) tail = tail.substring(0, tail.length() - 4);
                // if there’s a trailing slash, strip it
                tail = tail.replaceAll("/+$", "");
                if (tail.contains(".")) return tail;
            }
        }

        // Next: DOI prefix 10.18653/v1/<slug>
        if (p.doi() != null && p.doi().startsWith("10.18653/v1/")) {
            String tail = p.doi().substring("10.18653/v1/".length());
            if (tail.contains(".")) return tail;
        }

        // Sometimes id already is an ACL-style slug (contains dots)
        if (p.id() != null && p.id().contains(".")) return p.id();

        // Otherwise, no slug
        return null;
    }

    public static List<BenchmarkRecord> collate(List<PaperDto> candidates) {
//        List<BenchmarkRecord> out = new ArrayList<>();
//        for (var p : candidates) {
//            try {
//                out.add(extractFromPaper(p));
//            } catch (Exception ignored) {
//            }
//        }
//        return out;
        var cache = new Cache(Path.of(".cache/benchmarks"));
        var out = new java.util.ArrayList<BenchmarkRecord>();
        for (var p : candidates) {
            if (p == null) continue;
            String key = (p.openAlexId() != null && !p.openAlexId().isBlank()) ? p.openAlexId() :
                    (p.doi() != null && !p.doi().isBlank() ? p.doi() : String.valueOf(p.id()));
            try {
                var items = cache.getOrCompute(
                        key,
                        p.title(),
                        p.abs(),
                        () -> {
                            try {
                                return List.of(extractFromPaper(p));
                            } catch (Exception e) {
                                return List.of();
                            }
                        }
                );
                out.addAll(items);
            } catch (Exception ignored) {
            }
        }
        return out;
    }

    private static class Cache {
        private final Path dir;
        private final ObjectMapper M = new ObjectMapper();

        public Cache(Path dir) {
            this.dir = dir;
            try {
                Files.createDirectories(dir);
            } catch (IOException ignored) {
            }
        }

        private static String safeId(String openAlexIdOrDoi) {
            if (openAlexIdOrDoi == null || openAlexIdOrDoi.isBlank()) return "unknown";
            return Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(openAlexIdOrDoi.getBytes(StandardCharsets.UTF_8));
        }

        private static String contentHash(String title, String abs) {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                String s = (title == null ? "" : title) + "\n" + (abs == null ? "" : abs);
                byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));
                return Base64.getUrlEncoder().withoutPadding().encodeToString(dig);
            } catch (Exception e) {
                return Integer.toHexString(((title == null ? "" : title) + (abs == null ? "" : abs)).hashCode());
            }
        }

        private Path fileFor(String id) {
            return dir.resolve(safeId(id) + ".json");
        }

        /**
         * Get cached results for this paper if the content hash matches,
         * else compute via supplier and write to disk.
         */
        public <T> List<T> getOrCompute(String openAlexIdOrDoi, String title, String abs, Supplier<List<T>> computer) {
            String h = contentHash(title, abs);
            Path f = fileFor(openAlexIdOrDoi);
            // Try read
            if (Files.exists(f)) {
                try {
                    var root = M.readTree(Files.readString(f));
                    String storedHash = root.has("hash") && !root.get("hash").isNull() ? root.get("hash").asText() : null;
                    if (h.equals(storedHash) && root.has("items") && root.get("items").isArray()) {
                        return M.readerForListOf(Object.class).readValue(root.get("items"));
                    }
                } catch (Exception ignored) {
                }
            }
            // Compute & write
            List<T> items = computer.get();
            try {
                var node = Map.of("hash", h, "items", items);
                Files.writeString(f, M.writeValueAsString(node), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException ignored) {
            }
            return items;
        }
    }
}
