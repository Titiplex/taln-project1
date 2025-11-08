package udem.taln.classification;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultWeightedEdge;
import udem.taln.classification.graph.core.*;
import udem.taln.utils.FileService;
import udem.taln.utils.Maths;
import udem.taln.wrapper.dto.PaperDto;
import udem.taln.wrapper.vectors.VectorsWService;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class ClassificationService {
    private static final List<Pattern> POS = List.of(
            re("\\bclassification\\b"),
            re("\\bclassif(?:y|ier|ication)s?\\b"),
            re("multi[-\\s]?label"),
            re("multi[-\\s]?class"),
            re("\\bbinary\\b"),
            re("\\bsentiment\\b"),
            re("\\bstance\\b"),
            re("\\bintent\\b"),
            re("\\btopic\\b"),
            re("\\bemotion\\b"),
            re("hate\\s?speech"),
            re("\\btoxicit(y|ies)\\b"),
            re("\\bspam\\b"),
            re("\\bsarcasm\\b"),
            re("\\birony\\b"),
            re("\\bgenre\\b"),
            re("language identification"),
            re("dialect identification"),
            re("\\bNLI\\b"),
            re("natural language inference"),
            re("\\bentailment\\b"),
            re("\\bcontradiction\\b"),
            re("\\bneutral(ity)?\\b")
    );
    private static final List<Pattern> NEG = List.of(
            re("sequence\\s+labeling"),
            re("\\btagging\\b"),
            re("\\bNER\\b"),
            re("POS\\s+tagging"),
            re("parsing"),
            re("dependency\\s+parsing"),
            re("constituen(cy|ts)"),
            re("semantic\\s+role\\s+labeling"),
            re("coreference"),
            re("machine\\s+translation|\\bMT\\b"),
            re("summari[sz]ation"),
            re("generation"),
            re("speech\\s+recognition|\\bASR\\b")
    );
    private static final List<Pattern> BENCH = List.of(
            re("\\bdataset\\b"),
            re("\\bcorpus\\b"),
            re("\\bbenchmark\\b"),
            re("shared\\s+task"),
            re("task\\s+(overview|description)"),
            re("\\bcollection\\b"),
            re("\\breleased?\\b"),
            re("annotation\\s+guidelines"),
            re("gold\\s+standard"),
            re("train/?dev/?test\\s+split"),
            re("\\bleaderboard\\b")
    );

    private static Pattern
    re(String r) {
        return Pattern.compile(r, Pattern.CASE_INSENSITIVE);
    }

    public static List<PaperDto> filterEmptyTitles(List<PaperDto> papers) {
        return papers.stream().filter(paper -> paper.title() != null && !paper.title().isEmpty()).toList();
    }

    public static List<PaperDto> filterByCitationsNumber(List<PaperDto> papers, int max) {
        return papers.stream().filter(paper -> paper.citedByCount() != null && paper.citedByCount() <= max).toList();
    }

    public static List<PaperDto> filterEmptyAuthors(List<PaperDto> papers) {
        return papers.stream().filter(paper -> paper.authors() != null && !paper.authors().isEmpty()).toList();
    }

    public static List<PaperDto> filterEmptyDOIs(List<PaperDto> papers) {
        return papers.stream().filter(paper -> paper.doi() != null && !paper.doi().isEmpty()).toList();
    }

    public static List<PaperDto> filterEmptyPdfUrls(List<PaperDto> papers) {
        return papers.stream().filter(paper -> paper.pdfUrl() != null && !paper.pdfUrl().isEmpty()).toList();
    }

    public static List<PaperDto> filterEmptyAbstracts(List<PaperDto> papers) {
        return papers.stream().filter(paper -> paper.abs() != null && !paper.abs().isEmpty()).toList();
    }

    public static List<PaperDto> fill(List<PaperDto> papers) {
        OpenAlexService opAlex = new OpenAlexService("thinkerga@gmail.com", 24,
                200_000, Path.of(".cache/openalex"));
        opAlex.startRateLimiter(12, 24);
        Map<String, OpenAlexService.Tuple> fetched = opAlex.getByDoiBatchSmart(
                papers.stream().map(PaperDto::doi).toList(), 30);

        return papers.stream().map(p -> {
            String key = OpenAlexService.toCanonicalDoi(p.doi());
            OpenAlexService.Tuple t = (key == null) ? null : fetched.get(key);
            return new PaperDto(
                    p.id(), p.title(), p.abs(), p.year(), p.venue(), p.venueRaw(), p.pdfUrl(),
                    p.authors(), p.doi(),
                    (t != null ? t.openAlexId() : p.openAlexId()),
                    (t != null ? t.citedNumber() : p.citedByCount()),
                    isClassification(p.title(), p.abs()),
                    mentionsDatasetOrBenchmark(p.title(), p.abs()),
                    p.signals(),
                    (t != null ? t.referencedWorks() : List.of())
            );
        }).toList();
    }

    private static int hits(List<Pattern> ps, String s) {
        int c = 0;
        for (var p : ps) {
            var m = p.matcher(s);
            while (m.find()) c++;
        }
        return c;
    }

    public static boolean isClassification(String title, String abs) {
        String t = safe(title), a = safe(abs);
        int pos = 2 * hits(POS, t) + hits(POS, a);
        int neg = 2 * hits(NEG, t) + hits(NEG, a);
        int score = pos - neg;
        return score >= 2;
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }

    public static boolean mentionsDatasetOrBenchmark(String title, String abs) {
        String t = title == null ? "" : title;
        String a = abs == null ? "" : abs;
        return any(t) || any(a);
    }

    private static boolean any(String s) {
        for (var p : ClassificationService.BENCH) if (p.matcher(s).find()) return true;
        return false;
    }

    public static class Result {
        public Graph<String, DefaultWeightedEdge> fusedGraph;
        public Map<String, Integer> community;
        public Map<String, Double> finalScore;
        public List<String> topIds;
        public List<String> topDiverseIds;
        public List<PaperDto> topCurated;

        @Override
        public String toString() {
            return "Result{" +
                    "fusedGraph=" + fusedGraph +
                    ", community=" + community +
                    ", finalScore=" + finalScore +
                    ", topIds=" + topIds +
                    ", topDiverseIds=" + topDiverseIds +
                    ", topCurated=" + topCurated +
                    '}';
        }
    }

    public static Result run(List<PaperDto> papers) throws Exception {
        VectorsWService embedder = new VectorsWService();
        embedder.start();
        var cache = new EmbeddingCache(Path.of(".cache/embeddings"));
        System.out.println("Starting embedding");
        Map<String, float[]> embs = new HashMap<>();
        for (var p : papers) {
            String id = p.openAlexId();
            var text = (p.title() == null ? "" : p.title()) + ". " + (p.abs() == null ? "" : p.abs());
            float[] vec = cache.getOrCompute("paper:" + id, () -> embedder.getVector(text));
            embs.put(id, vec);
        }
        System.out.println("Done embedding");

        int dim = embs.values().iterator().next().length;
        var index = new KnnIndex(dim);
        for (var e : embs.entrySet()) index.add(e.getKey(), e.getValue());
        System.out.println("Done building index");

        var citation = GraphBuilder.buildCitationGraph(papers);
        System.out.println("Done building citation graph");
        var semantic = GraphBuilder.buildSemanticGraph(embs, index, /*topK=*/20, /*tau=*/0.40);
        System.out.println("Done building semantic graph");
        var fused = GraphBuilder.fuseGraphs(semantic, citation, /*alpha=*/1.0, /*beta=*/0.2);
        System.out.println("Done fusing graphs");

        var comm = LabelPropagation.detect(fused, 10);
        System.out.println("Done label propagation");

        String query = "text classification, datasets, benchmarks, NLP";
        float[] qv = embedder.getVector(query);
        Map<String, Double> simToQuery = new HashMap<>();
        for (var e : embs.entrySet()) {
            simToQuery.put(e.getKey(), Maths.cosine(e.getValue(), qv));
        }


        var pr = Ranking.pagerank(citation, 0.85);
        var recency = Ranking.recency(papers, 2025, 0.20);
        var logCites = Ranking.logCitations(papers);
        var venue = Ranking.venueBonus(papers, Set.of("acl", "emnlp", "naacl", "coling", "neurips", "icml"));
        var bench = Ranking.benchmarkFlag(papers);
        System.out.println("Done ranking");

        var score = Ranking.score(papers, simToQuery, pr, recency, logCites, venue, bench);
        System.out.println("Done ranking scores");

        Map<String, Map<String, String>> attrs = new HashMap<>();
        Map<String, PaperDto> byId = new HashMap<>();
        for (var p : papers) byId.put(p.openAlexId(), p);
        for (String id : fused.vertexSet()) {
            var p = byId.get(id);
            Map<String, String> a = new LinkedHashMap<>();
            a.put("label", p != null && p.title() != null ? p.title() : id);
            a.put("community", String.valueOf(comm.getOrDefault(id, -1)));
            a.put("score", String.format(Locale.ROOT, "%.6f", score.getOrDefault(id, 0.0)));
            a.put("year", String.valueOf(p != null ? p.year() : null));
            a.put("venue", p != null ? String.valueOf(p.venue()) : "");
            a.put("cited_by", String.valueOf(p != null ? p.citedByCount() : null));
            attrs.put(id, a);
        }

        System.out.printf("semantic edges=%d%n", semantic.edgeSet().size());
        System.out.printf("citation edges=%d%n", citation.edgeSet().size());
        System.out.printf("fused edges=%d%n", fused.edgeSet().size());

        var lite = GraphBuilder.topKSubgraph(fused, score, /*topK*/ 1200, /*minW*/ 0.40);
        System.out.printf("lite edges after weight cut=%d%n", lite.edgeSet().size());
        lite = GraphBuilder.copyOf(lite);
        lite = GraphBuilder.degreeCap(lite, 12);
        System.out.printf("lite edges after degree cap=%d%n", lite.edgeSet().size());

        analyse(lite, comm);

        Exporters.exportGraphML(fused, attrs, new File("data/graphs/graph-fused.graphml"));
        Exporters.exportNodesCsv(fused.vertexSet(), attrs::get, new File("data/graphs/nodes.csv"));
        Exporters.exportEdgesCsv(fused, new File("data/graphs/edges.csv"));
        System.out.println("Done exporting");

        Exporters.exportGraphML(lite, attrs, new File("data/graphs/graph-fused-lite.graphml"));
        Exporters.exportNodesCsv(lite.vertexSet(), attrs::get, new File("data/graphs/nodes-lite.csv"));
        Exporters.exportEdgesCsv(lite, new File("data/graphs/edges-lite.csv"));
        Exporters.exportLargestCommunities(lite, comm, /*howMany*/ 2, attrs::get);
        System.out.println("Done exporting LITE");

        embedder.close();

        var top100 = ResultOps.topN(score, 100);
        var top50div = ResultOps.diversifiedTop(score, comm, 50);
        var top50FilteredIds = ResultOps.filterRanked(top50div, byId, /*minYear*/ 2021, /*classif*/ true, /*dataset*/ true);
        var top50Filtered = ResultOps.materialize(top50FilteredIds, byId);

        var r = new Result();
        r.fusedGraph = fused;
        r.community = comm;
        r.finalScore = score;
        r.topIds = top100;
        r.topDiverseIds = top50div;
        r.topCurated = top50Filtered;
        return r;
    }

    private static void analyse(Graph<String, DefaultWeightedEdge> lite, Map<String, Integer> community) throws IOException {
        var liteStats = Metrics.basicStats(lite);
        System.out.printf("LITE |V|=%d |E|=%d avgDeg=%.2f avgW=%.3f%n",
                liteStats.nVertices(), liteStats.nEdges(), liteStats.avgDegree(), liteStats.avgEdgeWeight());

// Top-10 PageRank (damping=0.85)
        var prTop = Metrics.pageRankTopK(lite, 10, 0.85);
        System.out.println("Top-10 PageRank (lite):");
        prTop.forEach(e -> System.out.printf("  %s : %.5f%n", e.getKey(), e.getValue()));

// Top-5 par communauté
        Set<Integer> commLabels = new LinkedHashSet<>(community.values());
        for (int c : commLabels) {
            var sub = Metrics.communitySubgraph(lite, community, c);
            if (sub.vertexSet().isEmpty()) continue;
            var st = Metrics.basicStats(sub);
            System.out.printf("COMM %d |V|=%d |E|=%d avgDeg=%.2f avgW=%.3f%n",
                    c, st.nVertices(), st.nEdges(), st.avgDegree(), st.avgEdgeWeight());
            var top5 = Metrics.pageRankTopK(sub, 5, 0.85);
            System.out.println("  Top-5 PR:");
            top5.forEach(e -> System.out.printf("    %s : %.5f%n", e.getKey(), e.getValue()));
        }

// Modularity (pondérée) du partitionnement sur le lite
        double Q = Metrics.modularity(lite, community);
        System.out.printf("Modularity (lite, weighted): %.4f%n", Q);

// Approx. conductance entre deux plus grosses communautés (si >=2)
        List<Integer> biggest = new ArrayList<>(commLabels);
        biggest.sort((a, b) -> Long.compare(
                community.values().stream().filter(x -> Objects.equals(x, b)).count(),
                community.values().stream().filter(x -> Objects.equals(x, a)).count()));
        if (biggest.size() >= 2) {
            int c0 = biggest.get(0), c1 = biggest.get(1);
            var A = lite.vertexSet().stream().filter(v -> Objects.equals(community.get(v), c0)).collect(java.util.stream.Collectors.toSet());
            var B = lite.vertexSet().stream().filter(v -> Objects.equals(community.get(v), c1)).collect(java.util.stream.Collectors.toSet());
            double phi = Metrics.conductance(lite, A, B);
            System.out.printf("Conductance(comm %d, comm %d): %.4f%n", c0, c1, phi);
        }

// Assortativité par degré
        double r = Metrics.degreeAssortativity(lite);
        System.out.printf("Degree assortativity (lite): %.4f%n", r);

// Export CSV récapitulatif minimal
        var rows = new ArrayList<String[]>();
        rows.add(new String[]{"graph", "V", "E", "avg_degree", "avg_weight", "modularity", "assortativity"});
        rows.add(new String[]{"lite",
                String.valueOf(liteStats.nVertices()),
                String.valueOf(liteStats.nEdges()),
                String.format(java.util.Locale.ROOT, "%.4f", liteStats.avgDegree()),
                String.format(java.util.Locale.ROOT, "%.4f", liteStats.avgEdgeWeight()),
                String.format(java.util.Locale.ROOT, "%.4f", Q),
                String.format(java.util.Locale.ROOT, "%.4f", r)
        });
        FileService.writeCsv(new File("data/metrics/metrics_summary.csv"), rows);

// Export Top-10 PR global + Top-5 par communauté
        var rowsPr = new ArrayList<String[]>();
        rowsPr.add(new String[]{"scope", "rank", "openalex_id", "pagerank"});
        int rank = 1;
        for (var e : prTop)
            rowsPr.add(new String[]{"lite", String.valueOf(rank++), e.getKey(),
                    String.format(java.util.Locale.ROOT, "%.6f", e.getValue())});
        for (int c : commLabels) {
            var sub = Metrics.communitySubgraph(lite, community, c);
            var top = Metrics.pageRankTopK(sub, 5, 0.85);
            rank = 1;
            for (var e : top)
                rowsPr.add(new String[]{"comm_" + c, String.valueOf(rank++), e.getKey(),
                        String.format(java.util.Locale.ROOT, "%.6f", e.getValue())});
        }
        FileService.writeCsv(new File("data/metrics/top_pagerank.csv"), rowsPr);

    }

    public static class EmbeddingCache {
        private final Map<String, float[]> mem = new ConcurrentHashMap<>();
        private final Path dir;
        private final ObjectMapper M = new ObjectMapper();

        public EmbeddingCache(Path dir) {
            this.dir = dir;
            try {
                Files.createDirectories(dir);
            } catch (IOException ignored) {
            }
        }

        public float[] getOrCompute(String key, Supplier<float[]> computer) {
            var m = mem.get(key);
            if (m != null) return m;

            var d = readDisk(key);
            if (d != null) {
                mem.put(key, d);
                return d;
            }

            float[] v = computer.get();
            if (v != null) {
                mem.put(key, v);
                writeDisk(key, v);
            }
            return v;
        }

        public static String hashTextKey(String prefix, String text) {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                byte[] dig = md.digest(text.getBytes(StandardCharsets.UTF_8));
                String b64 = Base64.getUrlEncoder().withoutPadding().encodeToString(dig);
                return prefix + "_" + b64;
            } catch (Exception e) {
                return prefix + "_fallback_" + text.hashCode();
            }
        }

        private Path fileFor(String key) {
            // un nom safe
            return dir.resolve(Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(key.getBytes(StandardCharsets.UTF_8)) + ".json");
        }

        private float[] readDisk(String key) {
            Path f = fileFor(key);
            if (!Files.exists(f)) return null;
            try {
                var node = M.readTree(Files.readString(f));
                var arr = node.get("vector");
                if (arr == null || !arr.isArray()) return null;
                float[] v = new float[arr.size()];
                for (int i = 0; i < arr.size(); i++) v[i] = (float) arr.get(i).asDouble();
                return v;
            } catch (Exception ignore) {
                return null;
            }
        }

        private void writeDisk(String key, float[] vec) {
            Path f = fileFor(key);
            try {
                var map = Map.of("key", key, "vector", vec);
                Files.writeString(f, M.writeValueAsString(map),
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException ignored) {
            }
        }
    }

    public static final class ResultOps {

        public static List<String> topN(Map<String, Double> score, int n) {
            return score.entrySet().stream()
                    .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                    .limit(n)
                    .map(Map.Entry::getKey)
                    .toList();
        }

        public static List<String> diversifiedTop(Map<String, Double> score, Map<String, Integer> comm, int n) {
            Map<Integer, List<Map.Entry<String, Double>>> buckets = new HashMap<>();
            for (var e : score.entrySet()) {
                int c = comm.getOrDefault(e.getKey(), -1);
                buckets.computeIfAbsent(c, _ -> new ArrayList<>()).add(e);
            }
            for (var list : buckets.values()) {
                list.sort(Map.Entry.<String, Double>comparingByValue().reversed());
            }
            List<String> out = new ArrayList<>();
            var keys = new ArrayList<>(buckets.keySet());
            int idx = 0;
            while (out.size() < n && !buckets.isEmpty()) {
                if (keys.isEmpty()) keys = new ArrayList<>(buckets.keySet());
                if (keys.isEmpty()) break;
                int c = keys.get(idx % keys.size());
                var l = buckets.get(c);
                if (l == null || l.isEmpty()) {
                    buckets.remove(c);
                    keys.remove(Integer.valueOf(c));
                    continue;
                }
                out.add(l.removeFirst().getKey());
                idx++;
            }
            return out;
        }

        public static List<String> filterRanked(
                List<String> rankedIds,
                Map<String, PaperDto> byId,
                Integer minYear,
                boolean mustBeClassification,
                boolean mustMentionDataset
        ) {
            return rankedIds.stream().filter(id -> {
                var p = byId.get(id);
                if (p == null) return false;
                if (minYear != null && (p.year() == 0 || p.year() < minYear)) return false;
                if (mustBeClassification && !Boolean.TRUE.equals(p.isClassificationCandidate())) return false;
                return !mustMentionDataset || Boolean.TRUE.equals(p.isDatasetOrBenchmarkCandidate());
            }).toList();
        }

        public static List<PaperDto> materialize(List<String> ids, Map<String, PaperDto> byId) {
            List<PaperDto> out = new ArrayList<>();
            for (String id : ids) {
                var p = byId.get(id);
                if (p != null) out.add(p);
            }
            return out;
        }
    }
}
