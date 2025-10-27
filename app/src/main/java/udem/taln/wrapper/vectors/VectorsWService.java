package udem.taln.wrapper.vectors;

import udem.taln.wrapper.GenWAbstract;
import udem.taln.wrapper.parsers.WrapperParsers;

public class VectorsWService extends GenWAbstract<VecInterface> {
    public VectorsWService() {
        super("workers/embed/__main__.py");
    }

    public float[] getVector(String text) {
        return withRetry(() -> {
            var json = requiredPy().getVector(text);
            String preview = (json == null) ? "null" : json.replaceAll("\\s+", " ").trim();
            if (preview.length() > 200) preview = preview.substring(0, 200) + "...";
            System.out.println("[VectorsWService] Received JSON preview: " + preview);
            return WrapperParsers.parseVec(json);
        });
    }
}
