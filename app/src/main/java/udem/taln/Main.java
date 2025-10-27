package udem.taln;

import udem.taln.classification.ClassificationService;
import udem.taln.utils.FileService;
import udem.taln.wrapper.papers.PapersWService;
import udem.taln.wrapper.dto.PaperDto;

import java.io.File;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        PapersWService service = new PapersWService();
        service.start();
        System.out.println("Started service");
        System.out.println("PapersWService started");
        // Add a small inspection pass to see if some items have non-empty fields
        List<PaperDto> papers = List.of();
        try {
            papers = FileService.readJsonl(new File("data/raw/papers.jsonl").toPath());

            if (papers.isEmpty()) {
                System.out.println("No papers found in data/raw/papers.jsonl");
                papers = service.getPapers();
            }
        } catch (Exception e) {
            System.err.println("Failed to read papers.jsonl: " + e.getMessage());
        }
        service.close();
        System.out.println(papers.getFirst());
        long withAbstract = papers.stream().filter(p -> p.abs() != null && !p.abs().isBlank()).count();
        long withAuthors = papers.stream().filter(p -> p.authors() != null && !p.authors().isEmpty()).count();
        long withPdf = papers.stream().filter(p -> p.pdfUrl() != null && !p.pdfUrl().isBlank()).count();
        long withDOI = papers.stream().filter(p -> p.doi() != null && !p.doi().isBlank()).count();
        System.out.println("Stats -> total: " + papers.size() + ", withAbstract: " + withAbstract + ", withAuthors: " + withAuthors + ", withPdf: " + withPdf + ", withDOI: " + withDOI);

        papers = ClassificationService.filterEmptyTitles(papers);
        System.out.println("Filtered empty titles: " + papers.size());
        papers = ClassificationService.filterEmptyAuthors(papers);
        System.out.println("Filtered empty authors: " + papers.size());
        papers = ClassificationService.filterEmptyDOIs(papers);
        System.out.println("Filtered empty DOIs: " + papers.size());
        papers = ClassificationService.filterEmptyPdfUrls(papers);
        System.out.println("Filtered empty PDF URLs: " + papers.size());
        papers = ClassificationService.filterEmptyAbstracts(papers);
        System.out.println("Filtered empty abstracts: " + papers.size());
        long first = System.currentTimeMillis();
        papers = ClassificationService.fill(papers);
        System.out.println("Time to fill: " + (System.currentTimeMillis() - first));
        System.out.println("Filled: " + papers.size());
        papers = ClassificationService.filterByCitationsNumber(papers, 200);
        System.out.println("Filtered by citations: " + papers.size());
    }
}