package udem.taln.wrapper.dto;

import com.github.jelmerk.knn.Item;

public record VecItem(String id, float[] vector) implements Item<String, float[]> {
    @Override public int dimensions() { return vector.length; }
}