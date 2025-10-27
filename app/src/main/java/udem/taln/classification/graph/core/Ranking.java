package udem.taln.classification.graph.core;

import org.jgrapht.Graph;
import org.jgrapht.alg.scoring.PageRank;
import org.jgrapht.graph.DefaultWeightedEdge;
import udem.taln.wrapper.dto.PaperDto;

import java.util.*;

public class Ranking {

    public static Map<String, Double> pagerank(Graph<String, DefaultWeightedEdge> citation, double damping) {
        var pr = new PageRank<>(citation, damping);
        Map<String, Double> out = new HashMap<>();
        for (String v : citation.vertexSet()) out.put(v, pr.getVertexScore(v));
        return normalize(out);
    }

    public static Map<String, Double> logCitations(List<PaperDto> papers) {
        double min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;
        Map<String, Double> raw = new HashMap<>();
        for (var p : papers) {
            double v = Math.log1p(Math.max(0, Optional.ofNullable(p.citedByCount()).orElse(0)));
            raw.put(p.openAlexId(), v);
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        return normalize(raw, min, max);
    }

    public static Map<String, Double> recency(List<PaperDto> papers, int currentYear, double lambda) {
        Map<String, Double> map = new HashMap<>();
        for (var p : papers) {
            int y = Optional.of(p.year()).orElse(currentYear);
            double v = Math.exp(-lambda * Math.max(0, currentYear - y));
            map.put(p.openAlexId(), v);
        }
        return map; // déjà [0,1]
    }

    public static Map<String, Double> venueBonus(List<PaperDto> papers, Set<String> strongVenues) {
        Map<String, Double> map = new HashMap<>();
        for (var p : papers) {
            boolean hit = p.venue() != null && strongVenues.contains(p.venue().name().toLowerCase());
            map.put(p.openAlexId(), hit ? 1.0 : 0.0);
        }
        return map;
    }

    public static Map<String, Double> benchmarkFlag(List<PaperDto> papers) {
        Map<String, Double> map = new HashMap<>();
        for (var p : papers) map.put(p.openAlexId(), p.isDatasetOrBenchmarkCandidate() ? 1.0 : 0.0);
        return map;
    }

    public static Map<String, Double> normalize(Map<String, Double> m) {
        double min = m.values().stream().mapToDouble(d -> d).min().orElse(0);
        double max = m.values().stream().mapToDouble(d -> d).max().orElse(1);
        return normalize(m, min, max);
    }

    public static Map<String, Double> normalize(Map<String, Double> m, double min, double max) {
        double range = Math.max(1e-9, max - min);
        Map<String, Double> out = new HashMap<>(m.size());
        m.forEach((k, v) -> out.put(k, (v - min) / range));
        return out;
    }

    /**
     * score final
     */
    public static Map<String, Double> score(
            List<PaperDto> papers,
            Map<String, Double> simToQuery,
            Map<String, Double> pr,
            Map<String, Double> recency,
            Map<String, Double> logCitesNorm,
            Map<String, Double> venueBonus,
            Map<String, Double> benchFlag
    ) {
        Map<String, Double> out = new HashMap<>();
        for (var p : papers) {
            String id = p.openAlexId();
            double s =
                    0.45 * simToQuery.getOrDefault(id, 0.0) +
                            0.20 * pr.getOrDefault(id, 0.0) +
                            0.15 * recency.getOrDefault(id, 0.0) +
                            0.10 * logCitesNorm.getOrDefault(id, 0.0) +
                            0.07 * venueBonus.getOrDefault(id, 0.0) +
                            0.03 * benchFlag.getOrDefault(id, 0.0);
            out.put(id, s);
        }
        return out;
    }
}