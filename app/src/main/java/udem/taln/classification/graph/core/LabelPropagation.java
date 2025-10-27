package udem.taln.classification.graph.core;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultWeightedEdge;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class LabelPropagation {
    /**
     * Retourne map nodeId -> communityId
     */
    public static Map<String, Integer> detect(Graph<String, DefaultWeightedEdge> g, int maxIters) {
        List<String> nodes = new ArrayList<>(g.vertexSet());
        Map<String, Integer> label = new HashMap<>();
        for (int i = 0; i < nodes.size(); i++) label.put(nodes.get(i), i);

        for (int it = 0; it < maxIters; it++) {
            Collections.shuffle(nodes, ThreadLocalRandom.current());
            int changes = 0;
            for (String u : nodes) {
                Map<Integer, Double> score = new HashMap<>();
                for (var e : g.edgesOf(u)) {
                    String v = g.getEdgeSource(e).equals(u) ? g.getEdgeTarget(e) : g.getEdgeSource(e);
                    int lv = label.get(v);
                    double w = g.getEdgeWeight(e);
                    score.merge(lv, w, Double::sum);
                }
                if (score.isEmpty()) continue;
                int best = score.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();
                if (best != label.get(u)) {
                    label.put(u, best);
                    changes++;
                }
            }
            if (changes == 0) break;
        }
        Map<Integer, Integer> remap = new HashMap<>();
        int next = 0;
        for (int l : new HashSet<>(label.values())) remap.put(l, next++);
        label.replaceAll((k, v) -> remap.get(v));
        return label;
    }
}