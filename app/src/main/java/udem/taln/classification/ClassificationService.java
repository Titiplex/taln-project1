package udem.taln.classification;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultWeightedEdge;
import udem.taln.classification.graph.core.*;
import udem.taln.classification.graph.embed.EmbeddingService;
import udem.taln.classification.graph.embed.HttpEmbeddingService;
import udem.taln.utils.Maths;
import udem.taln.wrapper.dto.PaperDto;

import java.io.File;
import java.nio.file.Path;
import java.util.*;
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
    }

    public static Result run(List<PaperDto> papers) throws Exception {
        EmbeddingService embedder = new HttpEmbeddingService("http://localhost:8000/embed");
        Map<String, float[]> embs = new HashMap<>();
        for (var p : papers) {
            var text = (p.title() == null ? "" : p.title()) + ". " + (p.abs() == null ? "" : p.abs());
            embs.put(p.openAlexId(), embedder.embed(text));
        }

        int dim = embs.values().iterator().next().length;
        var index = new KnnIndex(dim);
        for (var e : embs.entrySet()) index.add(e.getKey(), e.getValue());

        var citation = GraphBuilder.buildCitationGraph(papers);
        var semantic = GraphBuilder.buildSemanticGraph(embs, index, /*topK=*/20, /*tau=*/0.40);
        var fused = GraphBuilder.fuseGraphs(semantic, citation, /*alpha=*/1.0, /*beta=*/0.2);

        var comm = LabelPropagation.detect(fused, 10);

        String query = "text classification, datasets, benchmarks, NLP";
        float[] qv = embedder.embed(query);
        Map<String, Double> simToQuery = new HashMap<>();
        for (var e : embs.entrySet()) {
            simToQuery.put(e.getKey(), Maths.cosine(e.getValue(), qv));
        }

        var pr = Ranking.pagerank(citation, 0.85);
        var recency = Ranking.recency(papers, 2025, 0.20);
        var logCites = Ranking.logCitations(papers);
        var venue = Ranking.venueBonus(papers, Set.of("acl", "emnlp", "naacl", "coling", "neurips", "icml"));
        var bench = Ranking.benchmarkFlag(papers);

        var score = Ranking.score(papers, simToQuery, pr, recency, logCites, venue, bench);

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
        Exporters.exportGraphML(fused, attrs, new File("graph-fused.graphml"));
        Exporters.exportNodesCsv(fused.vertexSet(), attrs::get, new File("nodes.csv"));
        Exporters.exportEdgesCsv(fused, new File("edges.csv"));

        var r = new Result();
        r.fusedGraph = fused;
        r.community = comm;
        r.finalScore = score;
        return r;
    }
}
