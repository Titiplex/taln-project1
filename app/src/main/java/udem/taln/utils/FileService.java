package udem.taln.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import udem.taln.wrapper.dto.PaperDto;
import udem.taln.wrapper.parsers.WrapperParsers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileService {
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void printToFile(String filePath, String s) throws IOException {
        File file = new File(filePath);
        Files.createDirectories(file.toPath().getParent());
        if (!file.exists() && file.createNewFile()) {
            file.setWritable(true);
            file.setReadable(true);
        } else {
            System.err.println("File already exists: " + filePath);
            return;
        }
        Writer writer = new FileWriter(file);
        writer.write(s);
        writer.close();
    }

    public static List<PaperDto> readJsonlPapers(Path path) throws IOException {
        if (!Files.exists(path)) return List.of();
        try (var r = Files.newBufferedReader(path)) {
            return WrapperParsers.parsePapers(r.lines().map(l -> l + "\n").reduce("", String::concat));
        }
    }

    public record OpenAlexCacheEntry(
            String doi,
            String openAlexId,
            Integer citedNumber,
            List<String> referencedWorks
    ) {
    }

    public static Map<String, OpenAlexCacheEntry> readJsonlResultByDoi(Path cacheDir) throws IOException {
        if (cacheDir == null || !Files.isDirectory(cacheDir)) return Map.of();

        try (Stream<Path> paths = Files.list(cacheDir)) {
            return paths
                    .filter(p -> p.getFileName().toString().endsWith(".json"))
                    .map(FileService::safeReadOne)
                    .filter(Objects::nonNull)
                    .filter(e -> e.doi != null && !e.doi.isBlank())
                    .collect(Collectors.toMap(
                            e -> e.doi,
                            e -> e,
                            (a, b) -> a,
                            LinkedHashMap::new
                    ));
        }
    }

    private static OpenAlexCacheEntry safeReadOne(Path file) {
        ObjectMapper M = new ObjectMapper();
        try {
            JsonNode node = M.readTree(Files.readString(file));

            String doi = asText(node.get("doi"));
            String openAlexId = asText(node.get("openAlexId"));
            Integer cited = node.has("citedNumber") && !node.get("citedNumber").isNull()
                    ? node.get("citedNumber").asInt()
                    : null;

            List<String> refs = new ArrayList<>();
            JsonNode rw = node.get("referencedWorks");
            if (rw != null && rw.isArray()) {
                for (JsonNode x : rw) {
                    String v = asText(x);
                    if (v != null) refs.add(v);
                }
            }
            return new OpenAlexCacheEntry(doi, openAlexId, cited, refs);
        } catch (Exception e) {
            return null;
        }
    }

    private static String asText(JsonNode n) {
        return (n == null || n.isNull()) ? null : n.asText();
    }

    public static void writeCsv(File f, List<String[]> rows) throws IOException {
        var sb = new StringBuilder();
        for (var r : rows) sb.append(String.join(",", r)).append("\n");
        Files.createDirectories(f.toPath().getParent());
        Files.writeString(f.toPath(), sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
