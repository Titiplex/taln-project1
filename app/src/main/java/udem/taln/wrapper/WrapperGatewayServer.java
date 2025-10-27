package udem.taln.wrapper;

import py4j.GatewayServer;

public class WrapperGatewayServer<T extends GenInterface> {
    private GatewayServer gateway;
    private EntryPoint<T> entry;

    public void start() {
        if (gateway != null) return;
        entry = new EntryPoint<>();
        gateway = new GatewayServer(entry); // port par défaut: 25333
        gateway.start();
        System.out.println("[Py4J] GatewayServer started on port " + gateway.getPort());
    }

    public void stop() {
        if (gateway != null) {
            gateway.shutdown();
            gateway = null;
            System.out.println("[Py4J] GatewayServer stopped");
        }
    }

    public EntryPoint<T> entry() {
        return entry;
    }
}