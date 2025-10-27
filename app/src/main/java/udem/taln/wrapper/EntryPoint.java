package udem.taln.wrapper;

public class EntryPoint<T extends GenInterface> {
    private volatile T py;

    /**
     * Is used in the python side, don't suppress
     *
     * @param obj
     */
    @SuppressWarnings("unused")
    public void registerPythonObject(T obj) {
        this.py = obj;
    }

    public boolean isPythonRegistered() {
        return py != null;
    }

    public T interfaceObject() {
        return py;
    }
}