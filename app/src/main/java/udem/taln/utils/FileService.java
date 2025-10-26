package udem.taln.utils;

import udem.taln.wrapper.dto.PaperDto;
import udem.taln.wrapper.parsers.WrapperParsers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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

    public static List<PaperDto> readJsonl(Path path) throws IOException {
        if (!Files.exists(path)) return List.of();
        try (var r = Files.newBufferedReader(path)) {
            return WrapperParsers.parsePapers(r.lines().map(l -> l + "\n").reduce("", String::concat));
        }
    }
}
