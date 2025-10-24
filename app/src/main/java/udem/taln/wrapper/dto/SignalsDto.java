package udem.taln.wrapper.dto;

import java.util.List;

public record SignalsDto(
        int score,                 // score final (+pos -neg +bonus)
        int posTitleHits,          // nb hits positifs dans le titre
        int posAbstractHits,       // nb hits positifs dans l’abstract
        int negTitleHits,          // nb hits d’exclusion dans le titre
        int negAbstractHits,       // nb hits d’exclusion dans l’abstract
        int datasetHits,           // nb hits de type dataset/benchmark
        List<String> matchedKeywords, // regex (ou libellés) ayant matché
        String rulesVersion        // "v1.0" -> si tu ajustes les règles
) {
}
