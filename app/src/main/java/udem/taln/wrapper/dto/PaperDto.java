package udem.taln.wrapper.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.util.List;

public record PaperDto(
        String id,                 // ex: "2023.acl-long.123" ou ID interne de l’Anthology
        String title,
        String abs,                // abstract (évite le mot-clé “abstract” en Java)
        int year,
        Venue venue,               // enum normalisé (ACL/NAACL/EACL/CONLL/EMNLP/COLING/LREC/FINDINGS)
        @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        List<String> venueRaw,     // étiquette brute (ex: ["Findings of ACL 2023"] ou ["acl"])
        String pdfUrl,
        List<AuthorDto> authors,

        // Enrichissements (remplis après via OpenAlex/Crossref)
        String doi,
        String openAlexId,
        Integer citedByCount,

        // Flags/metadata internes
        Boolean isClassificationCandidate,
        Boolean isDatasetOrBenchmarkCandidate,
        SignalsDto signals
) {
}
