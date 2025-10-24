package udem.taln.wrapper;

public class EntryPoint {
    private volatile ACLInterface py;

    public void registerPythonObject(ACLInterface obj) {
        this.py = obj;
    }

    public boolean isPythonRegistered() {
        return py != null;
    }

    public ACLInterface acl() {
        return py;
    }
}