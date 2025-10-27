package udem.taln.classification.graph.core;

import com.github.jelmerk.knn.DistanceFunctions;
import com.github.jelmerk.knn.SearchResult;
import com.github.jelmerk.knn.hnsw.HnswIndex;
import udem.taln.wrapper.dto.VecItem;

import java.util.Arrays;
import java.util.List;

public class KnnIndex {

    private final HnswIndex<String, float[], VecItem, Float> index;

    public KnnIndex(int dim) {
        this.index = HnswIndex
                .newBuilder(dim, DistanceFunctions.FLOAT_COSINE_DISTANCE, 100_000)
                .withM(32)
                .withEf(200)
                .withEfConstruction(200)
                .build();
    }

    public void add(String id, float[] vec) throws Exception {
        index.add(new VecItem(id, vec));
    }

    // k-NN par vecteur, en excluant l'item lui-même si l'id est fourni
    public List<SearchResult<VecItem, Float>> knn(String id, float[] vec, int k) {
        var res = index.findNeighbors(Arrays.toString(vec), k + 1);
        return res.stream()
                .filter(r -> !r.item().id().equals(id))
                .limit(k)
                .toList();
    }
}