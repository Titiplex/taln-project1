package udem.taln.wrapper.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import udem.taln.wrapper.dto.PaperDto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WrapperParsers {

    private static final ObjectMapper MAPPER_PAPERS = new ObjectMapper();
    private static final ObjectMapper MAPPER_VEC = new ObjectMapper();

    static {
        MAPPER_PAPERS.findAndRegisterModules();
        MAPPER_VEC.findAndRegisterModules();
    }

    public static List<PaperDto> parsePapers(String json) {
        try {
            var type = MAPPER_PAPERS.getTypeFactory().constructParametricType(List.class, PaperDto.class);
            JsonNode node = MAPPER_PAPERS.readTree(json); // could be {"papers":[...]} or [...]
            if (node == null) {
                throw new RuntimeException("Empty JSON payload");
            }
            if (node.isObject()) {
                JsonNode papers = node.get("papers");
                if (papers != null && papers.isArray()) {
                    return MAPPER_PAPERS.readValue(papers.traverse(), type);
                }
                throw new RuntimeException("JSON object missing 'papers' array field. Keys: " + node.fieldNames().toString());
            } else if (node.isArray()) {
                return MAPPER_PAPERS.readValue(node.traverse(), type);
            } else {
                // Attempt to treat as JSON Lines (each line an object)
                var lines = json.split("\\R");
                List<PaperDto> out = new ArrayList<>();
                for (String line : lines) {
                    String trimmed = line.trim();
                    if (trimmed.isEmpty()) continue;
                    JsonNode ln = MAPPER_PAPERS.readTree(trimmed);
                    if (!ln.isObject()) {
                        throw new RuntimeException("Unexpected JSON line (not an object): " + preview(trimmed, 160));
                    }
                    out.add(MAPPER_PAPERS.treeToValue(ln, PaperDto.class));
                }
                if (!out.isEmpty()) return out;
                throw new RuntimeException("Unsupported JSON root type: " + node.getNodeType());
            }
        } catch (IOException e) {
            String pv = preview(json, 200);
            throw new RuntimeException("Failed to parse papers JSON. Preview: " + pv, e);
        }
    }

    private static String preview(String s, int max) {
        if (s == null) return "null";
        s = s.replaceAll("\\s+", " ").trim();
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    public static float[] parseVec(String json) {
        try {
            var node = MAPPER_VEC.readTree(json).get("vector");
            float[] v = new float[node.size()];
            for (int i = 0; i < v.length; i++) v[i] = (float) node.get(i).asDouble();
            return v;
        } catch (IOException e) {
            String pv = preview(json, 200);
            throw new RuntimeException("Failed to parse papers JSON. Preview: " + pv, e);
        }
    }
}