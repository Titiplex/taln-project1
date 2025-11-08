package udem.taln;

import com.fasterxml.jackson.databind.ObjectMapper;
import udem.taln.classification.ClassificationService;
import udem.taln.classification.benchmark.Benchmark;
import udem.taln.utils.FileService;
import udem.taln.wrapper.dto.PaperDto;
import udem.taln.wrapper.papers.PapersWService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
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
            papers = FileService.readJsonlPapers(new File("data/raw/papers.jsonl").toPath());

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

        List<PaperDto> cached = new ArrayList<>();
        List<PaperDto> toFetch = new ArrayList<>();
        try {
            var r = FileService.readJsonlResultByDoi(new File(".cache/openalex").toPath());
            papers.forEach(p -> {
                if (r.containsKey(p.doi())) {
                    var temp = r.get(p.doi());
                    cached.add(new PaperDto(
                            p.id(),
                            p.title(),
                            p.abs(),
                            p.year(),
                            p.venue(),
                            p.venueRaw(),
                            p.pdfUrl(),
                            p.authors(),
                            p.doi(),
                            temp.openAlexId(),
                            temp.citedNumber(),
                            ClassificationService.isClassification(p.title(), p.abs()),
                            ClassificationService.mentionsDatasetOrBenchmark(p.title(), p.abs()),
                            p.signals(),
                            temp.referencedWorks()
                    ));
                } else {
                    toFetch.add(p);
                }
            });
            System.out.println("Cached : " + cached.size());
        } catch (IOException e) {
            System.err.println("Failed to read .cache/openalex: " + e.getMessage());
        }

        long first = System.currentTimeMillis();
        List<PaperDto> total = new ArrayList<>(ClassificationService.fill(toFetch));
        System.out.println("Time to fill: " + (System.currentTimeMillis() - first));
        System.out.println("Filled: " + total.size());
        total.addAll(cached);
        total = ClassificationService.filterByCitationsNumber(total, 200);
        System.out.println("Filtered by citations: " + total.size());

        try {
            System.out.println("Creating graph and classifying...");
            var res = ClassificationService.run(total);
            System.out.println("Done classifying");
            System.out.println("Top 10 global:");
            res.topIds.stream().limit(10).forEach(System.out::println);

            System.out.println("Top 10 diversified:");
            res.topDiverseIds.stream().limit(10).forEach(System.out::println);

            System.out.println("Top 10 filtered (recents + classification + dataset):");
            res.topCurated.stream().limit(10).forEach(p -> System.out.println(p.doi() + " : " + p.title()));

            // keep only the top 50 to see
            var curated = res.topCurated.subList(0, Math.min(50, res.topCurated.size()));

            var benchmarks = Benchmark.collate(curated);
            // write table for curated benchmarks
            try {
                var mapper = new ObjectMapper();
                var outPath = Path.of("data/benchmarks/benchmarks_curated.json");
                Files.createDirectories(outPath.getParent());
                Files.writeString(
                        outPath,
                        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(benchmarks),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING
                );
                System.out.println("Wrote curated benchmark records -> " + outPath);
            } catch (Exception e) {
                System.err.println("Failed to write benchmarks_curated.json: " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Failed to run classification: " + e.getMessage());
        }
    }
}