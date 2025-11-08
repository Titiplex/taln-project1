package udem.taln.classification.graph.core;

import org.jgrapht.Graph;
import org.jgrapht.graph.AsSubgraph;
import org.jgrapht.graph.DefaultWeightedEdge;

import java.util.*;
import java.util.stream.Collectors;

public final class Metrics {

    public record BasicStats(
            int nVertices,
            int nEdges,
            double avgDegree,
            double avgEdgeWeight
    ) {
    }

    /**
     * Stats de base (suppose graphe non orienté, comme le fused/lite).
     */
    public static BasicStats basicStats(Graph<String, DefaultWeightedEdge> g) {
        int n = g.vertexSet().size();
        int m = g.edgeSet().size();
        double sumW = g.edgeSet().stream().mapToDouble(g::getEdgeWeight).sum();
        double avgDeg = (n == 0) ? 0.0 : (2.0 * m) / n;
        double avgW = (m == 0) ? 0.0 : (sumW / m);
        return new BasicStats(n, m, avgDeg, avgW);
    }

    /**
     * Top-K PageRank (simple, non pondéré par poids d’arête).
     */
    public static List<Map.Entry<String, Double>> pageRankTopK(Graph<String, DefaultWeightedEdge> g, int k, double d) {
        var pr = new org.jgrapht.alg.scoring.PageRank<>(g, d);
        return pr.getScores().entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(k)
                .toList();
    }

    /**
     * Top-K par degré (fallback/complément).
     */
    public static List<Map.Entry<String, Integer>> degreeTopK(Graph<String, DefaultWeightedEdge> g, int k) {
        return g.vertexSet().stream()
                .map(v -> Map.entry(v, g.degreeOf(v)))
                .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                .limit(k)
                .toList();
    }

    /**
     * Sous-graphe d’une communauté.
     */
    public static Graph<String, DefaultWeightedEdge> communitySubgraph(
            Graph<String, DefaultWeightedEdge> g, Map<String, Integer> comm, int label
    ) {
        var keep = g.vertexSet().stream().filter(v -> Objects.equals(comm.get(v), label)).collect(Collectors.toSet());
        return new AsSubgraph<>(g, keep);
    }

    /**
     * Conductance entre deux ensembles (poids).
     */
    public static double conductance(Graph<String, DefaultWeightedEdge> g, Set<String> A, Set<String> B) {
        double cut = 0.0, volA = 0.0, volB = 0.0;
        for (var e : g.edgeSet()) {
            String u = g.getEdgeSource(e), v = g.getEdgeTarget(e);
            double w = g.getEdgeWeight(e);
            boolean uA = A.contains(u), vA = A.contains(v);
            boolean uB = B.contains(u), vB = B.contains(v);
            if (uA) volA += w;
            if (vA) volA += w;
            if (uB) volB += w;
            if (vB) volB += w;
            if ((uA && vB) || (uB && vA)) cut += w;
        }
        double denom = Math.max(1e-9, Math.min(volA, volB));
        return cut / denom;
    }

    /**
     * Modularity (version pondérée) d’un partitionnement.
     */
    public static double modularity(Graph<String, DefaultWeightedEdge> g, Map<String, Integer> comm) {
        // 2m = somme des poids (chaque arête comptée une fois) * 2
        double m2 = 2.0 * g.edgeSet().stream().mapToDouble(g::getEdgeWeight).sum();
        if (m2 <= 0) return 0.0;

        // degrés pondérés
        Map<String, Double> k = new HashMap<>();
        for (String v : g.vertexSet()) {
            double kv = 0.0;
            for (var e : g.edgesOf(v)) kv += g.getEdgeWeight(e);
            k.put(v, kv);
        }

        double Q = 0.0;
        for (var e : g.edgeSet()) {
            String u = g.getEdgeSource(e), v = g.getEdgeTarget(e);
            if (!Objects.equals(comm.get(u), comm.get(v))) continue;
            double Auv = g.getEdgeWeight(e);
            Q += (Auv - (k.get(u) * k.get(v) / (m2 / 2.0))); // (m2/2)=2m/2=m
        }
        return Q / (m2 / 2.0);
    }

    /**
     * Assortativité par degré (corrélation de Pearson des degrés aux extrémités des arêtes).
     */
    public static double degreeAssortativity(Graph<String, DefaultWeightedEdge> g) {
        List<double[]> pairs = new ArrayList<>();
        for (var e : g.edgeSet()) {
            pairs.add(new double[]{g.degreeOf(g.getEdgeSource(e)), g.degreeOf(g.getEdgeTarget(e))});
        }
        if (pairs.size() < 2) return 0.0;
        double meanX = pairs.stream().mapToDouble(p -> p[0]).average().orElse(0);
        double meanY = pairs.stream().mapToDouble(p -> p[1]).average().orElse(0);
        double num = 0, denX = 0, denY = 0;
        for (var p : pairs) {
            double dx = p[0] - meanX, dy = p[1] - meanY;
            num += dx * dy;
            denX += dx * dx;
            denY += dy * dy;
        }
        double den = Math.sqrt(denX * denY);
        return (den == 0) ? 0.0 : (num / den);
    }
}
