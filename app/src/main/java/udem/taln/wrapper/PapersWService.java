package udem.taln.wrapper;

import udem.taln.utils.FileService;
import udem.taln.wrapper.dto.PaperDto;
import udem.taln.wrapper.parsers.WrapperParsers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

public class PapersWService implements AutoCloseable {
    private final WrapperGatewayServer gateway = new WrapperGatewayServer();
    private final PythonLauncher python;
    private volatile boolean started;

    public PapersWService(String pythonCmd, String pythonScriptPath) {
        this.python = new PythonLauncher(pythonCmd, new File(pythonScriptPath));
    }

    public PapersWService() {
        this.python = new PythonLauncher();
    }

    /**
     * Starts Gateway + Python
     */
    public synchronized void start() {
        if (started) return;
        gateway.start();
        try {
            python.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Python", e);
        }
        // Increased wait time and added retry logic for Python registration
        if (!waitForPythonRegistration(Duration.ofSeconds(30))) {
            boolean alive = python.isAlive();
            throw new RuntimeException("Python failed to register within timeout period. Python alive=" + alive);
        }
        started = true;

        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    /**
     * Wait for Python to register with the gateway with retry logic
     */
    private boolean waitForPythonRegistration(Duration timeout) {
        long endTime = System.currentTimeMillis() + timeout.toMillis();
        long lastLog = 0;
        while (System.currentTimeMillis() < endTime) {
            try {
                if (gateway.entry().isPythonRegistered()) {
                    return true;
                }
                // If Python died, abort early with a clear message
                if (!python.isAlive()) {
                    return false;
                }
                if (System.currentTimeMillis() - lastLog > 2000) {
                    System.out.println("[PapersWService] Waiting for Python to register...");
                    lastLog = System.currentTimeMillis();
                }
                Thread.sleep(500);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception ignore) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }

    public List<PaperDto> getPapers() {
        return withRetry(() -> {
            var json = requiredPy().getPapers(1976, 2025);
            String preview = (json == null) ? "null" : json.replaceAll("\\s+", " ").trim();
            if (preview.length() > 200) preview = preview.substring(0, 200) + "...";
            System.out.println("[PapersWService] Received JSON preview: " + preview);
            if (json != null) {
                try {
                    FileService.printToFile("data/raw/papers.jsonl", json);
                } catch (IOException e) {
                    System.err.println("[PapersWService] Failed to write papers.jsonl: " + e.getMessage());
                }
            }
            return WrapperParsers.parsePapers(json);
        });
    }

    private ACLInterface requiredPy() {
        var ep = gateway.entry();
        if (ep == null || !ep.isPythonRegistered()) {
            throw new IllegalStateException("Python object not registered yet. Ensure Python process is running and spaCy models are loaded.");
        }
        return ep.acl();
    }

    /**
     * 3 times retry to support slowness and occasional errors.
     */
    private static <T> T withRetry(Supplier<T> fn) {
        RuntimeException last = null;
        for (int i = 0; i < 3; i++) {
            try {
                return fn.get();
            } catch (RuntimeException e) {
                last = e;
                try {
                    Thread.sleep(250L * (i + 1));
                } catch (InterruptedException ignored) {
                }
            }
        }
        throw last;
    }

    @Override
    public synchronized void close() {
        if (!started) return;
        try {
            python.stop();
        } catch (Exception ignored) {
        }
        try {
            gateway.stop();
        } catch (Exception ignored) {
        }
        started = false;
    }
}