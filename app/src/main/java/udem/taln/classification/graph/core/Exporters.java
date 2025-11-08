package udem.taln.classification.graph.core;

import org.jgrapht.Graph;
import org.jgrapht.graph.AsSubgraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.graphml.GraphMLExporter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.function.Function;

public class Exporters {
    public static void exportGraphML(
            Graph<String, DefaultWeightedEdge> g,
            Map<String, Map<String, String>> nodeAttrs,
            File out
    ) throws IOException {
        var exporter = new GraphMLExporter<String, DefaultWeightedEdge>();
        exporter.setExportEdgeWeights(true);
        exporter.setVertexAttributeProvider(v -> {
            Map<String, Attribute> m = new LinkedHashMap<>();
            Map<String, String> attrs = nodeAttrs.getOrDefault(v, Map.of());
            attrs.forEach((k, val) -> m.put(k, DefaultAttribute.createAttribute(val)));
            return m;
        });
        try (var w = new FileWriter(out)) {
            exporter.exportGraph(g, w);
        }
    }

    public static void exportNodesCsv(
            Collection<String> vertices,
            Function<String, Map<String, String>> attr,
            File out
    ) throws IOException {
        try (var w = new PrintWriter(out, java.nio.charset.StandardCharsets.UTF_8)) {
            w.println("id,label,community,score,year,venue,cited_by");
            for (String v : vertices) {
                Map<String, String> a = attr.apply(v);
                w.printf("%s,%s,%s,%s,%s,%s,%s%n",
                        q(v), q(a.getOrDefault("label", "")),
                        q(a.getOrDefault("community", "-1")),
                        q(a.getOrDefault("score", "0")),
                        q(a.getOrDefault("year", "")),
                        q(a.getOrDefault("venue", "")),
                        q(a.getOrDefault("cited_by", ""))
                );
            }
        }
    }

    public static void exportEdgesCsv(Graph<String, DefaultWeightedEdge> g, File out) throws IOException {
        try (var w = new PrintWriter(out, java.nio.charset.StandardCharsets.UTF_8)) {
            w.println("source,target,weight");
            for (var e : g.edgeSet()) {
                String u = g.getEdgeSource(e), v = g.getEdgeTarget(e);
                w.printf("%s,%s,%.6f%n", q(u), q(v), g.getEdgeWeight(e));
            }
        }
    }

    public static void exportLargestCommunities(
            Graph<String, DefaultWeightedEdge> g,
            Map<String, Integer> comm,
            int howMany,
            java.util.function.Function<String, Map<String, String>> attrsProvider
    ) throws IOException {
        Map<Integer, List<String>> byC = new HashMap<>();
        for (var e : comm.entrySet()) byC.computeIfAbsent(e.getValue(), c -> new ArrayList<>()).add(e.getKey());

        var ordered = byC.entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()))
                .limit(howMany)
                .toList();

        int rank = 0;
        for (var entry : ordered) {
            var nodes = new LinkedHashSet<>(entry.getValue());
            var sub = new AsSubgraph<>(g, nodes);
            Exporters.exportGraphML(sub, collectAttrs(nodes, attrsProvider), new File("data/graphs/graph-comm-" + (rank++) + ".graphml"));
        }
    }

    private static Map<String, Map<String, String>> collectAttrs(Set<String> nodes, java.util.function.Function<String, Map<String, String>> attrsProvider) {
        Map<String, Map<String, String>> m = new HashMap<>();
        for (String id : nodes) m.put(id, attrsProvider.apply(id));
        return m;
    }

    private static String q(String s) {
        if (s == null) return "";
        if (s.contains(",") || s.contains("\"")) return "\"" + s.replace("\"", "\"\"") + "\"";
        return s;
    }
}