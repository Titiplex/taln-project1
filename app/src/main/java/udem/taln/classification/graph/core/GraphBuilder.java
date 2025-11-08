package udem.taln.classification.graph.core;

import org.jgrapht.Graph;
import org.jgrapht.graph.AsSubgraph;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;
import udem.taln.wrapper.dto.PaperDto;

import java.util.*;
import java.util.stream.Collectors;

public class GraphBuilder {

    /**
     * Citation graph : oriented, weight = 1.0
     */
    public static Graph<String, DefaultWeightedEdge> buildCitationGraph(List<PaperDto> papers) {
        var g = new DefaultDirectedWeightedGraph<String, DefaultWeightedEdge>(DefaultWeightedEdge.class);
        for (var p : papers) g.addVertex(p.openAlexId());
        Set<String> set = g.vertexSet();
        for (var p : papers) {
            if (p.referencedWorks() == null) continue;
            for (String ref : p.referencedWorks()) {
                if (ref.equals(p.openAlexId())) continue;
                if (set.contains(ref)) {
                    var e = g.addEdge(p.openAlexId(), ref);
                    if (e != null) g.setEdgeWeight(e, 1.0);
                }
            }
        }
        return g;
    }

    /**
     * Semantic graph : undirected, weight = cosine sim, only keep topK neighbors >= tau
     */
    public static Graph<String, DefaultWeightedEdge> buildSemanticGraph(
            Map<String, float[]> embeddings,
            KnnIndex knn,
            int topK,
            double tau
    ) {
        var g = new SimpleWeightedGraph<String, DefaultWeightedEdge>(DefaultWeightedEdge.class);
        embeddings.keySet().forEach(g::addVertex);

        embeddings.forEach((id, vec) -> {
            var hits = knn.knn(id, vec, topK + 1); // +1 to skip self
            for (var h : hits) {
                String nb = h.item().id();
                if (nb.equals(id)) continue;
                double sim = 1.0 - h.distance(); // distance = 1-cosine
                if (sim >= tau) {
                    var e = g.addEdge(id, nb);
                    if (e != null) g.setEdgeWeight(e, sim);
                }
            }
        });
        return g;
    }

    /**
     * Fusion : poids = alpha*semantic + beta*citationAsUndirected(=1 si existe)
     */
    public static Graph<String, DefaultWeightedEdge> fuseGraphs(
            Graph<String, DefaultWeightedEdge> semantic,
            Graph<String, DefaultWeightedEdge> citation,
            double alpha, double beta
    ) {
        var fused = new SimpleWeightedGraph<String, DefaultWeightedEdge>(DefaultWeightedEdge.class);
        semantic.vertexSet().forEach(fused::addVertex);

        // semantic edges
        for (var e : semantic.edgeSet()) {
            var u = semantic.getEdgeSource(e);
            var v = semantic.getEdgeTarget(e);
            if (u.equals(v)) continue;
            double w = semantic.getEdgeWeight(e) * alpha;

            var fe = fused.getEdge(u, v);
            if (fe == null) fe = fused.addEdge(u, v);
            if (fe != null) fused.setEdgeWeight(fe, fused.getEdgeWeight(fe) + w);
        }

        // citation edges -> undirected presence with weight=beta
        for (var e : citation.edgeSet()) {
            var u = citation.getEdgeSource(e);
            var v = citation.getEdgeTarget(e);
            if (u.equals(v)) continue;
            if (!fused.containsVertex(u) || !fused.containsVertex(v)) continue;

            var fe = fused.getEdge(u, v);
            if (fe == null) fe = fused.addEdge(u, v);
            if (fe != null) fused.setEdgeWeight(fe, fused.getEdgeWeight(fe) + beta);
        }

        return fused;
    }

    /**
     * Garde les topK noeuds par score, et coupe les arêtes < minW.
     */
    public static Graph<String, DefaultWeightedEdge> topKSubgraph(
            Graph<String, DefaultWeightedEdge> g,
            Map<String, Double> score,
            int topK,
            double minW
    ) {
        var topNodes = score.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(topK)
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        var sub = new AsSubgraph<>(g, topNodes);

        var toDrop = new ArrayList<DefaultWeightedEdge>();
        for (var e : sub.edgeSet()) {
            if (sub.getEdgeWeight(e) < minW) toDrop.add(e);
        }
        toDrop.forEach(sub::removeEdge);
        return sub;
    }

    /**
     * Ne garde que m voisins plus forts par sommet (sur poids d’arête).
     */
    public static Graph<String, DefaultWeightedEdge> degreeCap(
            Graph<String, DefaultWeightedEdge> g,
            int m
    ) {
        var sub = new AsSubgraph<>(g, new LinkedHashSet<>(g.vertexSet()), new LinkedHashSet<>(g.edgeSet()));
        for (String v : new ArrayList<>(sub.vertexSet())) {
            var nbrs = sub.edgesOf(v).stream()
                    .sorted((e1, e2) -> Double.compare(sub.getEdgeWeight(e2), sub.getEdgeWeight(e1)))
                    .toList();
            for (int i = m; i < nbrs.size(); i++) sub.removeEdge(nbrs.get(i));
        }
        return sub;
    }

    public static Graph<String, DefaultWeightedEdge> copyOf(Graph<String, DefaultWeightedEdge> g) {
        var h = new SimpleWeightedGraph<String, DefaultWeightedEdge>(DefaultWeightedEdge.class);
        g.vertexSet().forEach(h::addVertex);
        for (var e : g.edgeSet()) {
            var u = g.getEdgeSource(e);
            var v = g.getEdgeTarget(e);
            var ne = h.addEdge(u, v);
            if (ne != null) h.setEdgeWeight(ne, g.getEdgeWeight(e));
        }
        return h;
    }
}