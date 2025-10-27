package udem.taln.wrapper.papers;

import udem.taln.utils.FileService;
import udem.taln.wrapper.GenWAbstract;
import udem.taln.wrapper.dto.PaperDto;
import udem.taln.wrapper.parsers.WrapperParsers;

import java.io.IOException;
import java.util.List;

public class PapersWService extends GenWAbstract<ACLInterface> {

    public PapersWService() {
        super("workers/harvest/__main__.py");
    }

    public List<PaperDto> getPapers() {
        return withRetry(() -> {
            var json = requiredPy().getPapers(1976, 2025);
            String preview = (json == null) ? "null" : json.replaceAll("\\s+", " ").trim();
            if (preview.length() > 200) preview = preview.substring(0, 200) + "...";
            System.out.println("[PapersWService] Received JSON preview: " + preview);
            if (json != null) {
                try {
                    FileService.printToFile("data/raw/papers.jsonl", json);
                } catch (IOException e) {
                    System.err.println("[PapersWService] Failed to write papers.jsonl: " + e.getMessage());
                }
            }
            return WrapperParsers.parsePapers(json);
        });
    }
}