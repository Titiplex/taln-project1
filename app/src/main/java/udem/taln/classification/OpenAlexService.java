package udem.taln.classification;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;

public class OpenAlexService {
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20)).build();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public record Tuple (String openAlexId, Integer citedNumber) {}

    public Optional<Tuple> getByDoi(String rawDoi) {
        String doi = normalizeDoi(rawDoi);
        if (doi == null) return Optional.empty();
        String url = "https://api.openalex.org/works/doi:" + doi;
        return fetch(url);
    }

    private static String normalizeDoi(String raw) {
        if (raw == null) return null;
        String d = raw.trim().toLowerCase();
        d = d.replaceFirst("^https?://(dx\\.)?doi\\.org/", "");
        if (d.isBlank()) return null;
        return java.net.URLEncoder.encode(d, StandardCharsets.UTF_8);
    }

    private Optional<Tuple> fetch(String url) {
        try {
            var node = getJson(url);
            if (node == null || node.has("error")) return Optional.empty();
            String id = asText(node.get("id"));
            Integer cited = node.has("cited_by_count") ? node.get("cited_by_count").asInt() : null;
            if (id == null) return Optional.empty();
            String openAlexId = id.substring(id.lastIndexOf('/') + 1);
            return Optional.of(new Tuple(openAlexId, cited));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static String asText(JsonNode n) {
        return (n == null || n.isNull()) ? null : n.asText();
    }

    private JsonNode getJson(String url) throws Exception {
        int tries = 0;
        long sleep = 200;
        while (true) {
            tries++;
            var req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(30)).GET().build();
            var res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            int sc = res.statusCode();
            if (sc == 200) return MAPPER.readTree(res.body());
            if ((sc == 429 || sc >= 500) && tries < 6) {
                Thread.sleep(sleep);
                sleep *= 2;
                continue;
            }
            return null;
        }
    }

    // TODO
//    public Optional<Tuple> searchByTitleYear(String title, int year) {
//        if (title == null || title.isBlank()) return Optional.empty();
//        String q = java.net.URLEncoder.encode(title + " " + year, java.nio.charset.StandardCharsets.UTF_8);
//        String url = "https://api.openalex.org/works?search=" + q + "&per_page=3&mailto=" + mailto;
//        try {
//            var node = getJson(url);
//            if (node == null || !node.has("results")) return Optional.empty();
//            var results = node.get("results");
//            double bestScore = 0; Tuple best = null;
//            String tNorm = TextUtil.normTitle(title);
//            for (var it : results) {
//                String id = asText(it.get("id"));              // "https://openalex.org/Wxxxx"
//                String t2 = asText(it.get("title"));
//                int y2 = it.has("publication_year") ? it.get("publication_year").asInt() : 0;
//                Integer cited = it.has("cited_by_count") ? it.get("cited_by_count").asInt() : null;
//                if (id == null || t2 == null) continue;
//                String openAlexId = id.substring(id.lastIndexOf('/') + 1);
//                double sim = TextUtil.titleSimilarity(tNorm, TextUtil.normTitle(t2));
//                boolean yearOk = (year == 0 || y2 == 0 || Math.abs(year - y2) <= 1);
//                if (yearOk && sim > bestScore && sim >= 0.90) {
//                    bestScore = sim;
//                    best = new Tuple(openAlexId, cited);
//                }
//            }
//            return Optional.ofNullable(best);
//        } catch (Exception e) {
//            return Optional.empty();
//        }
//    }
}
