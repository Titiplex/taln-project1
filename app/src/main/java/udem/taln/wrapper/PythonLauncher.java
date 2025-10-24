package udem.taln.wrapper;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PythonLauncher {
    private final String PYTHON_CMD;
    private final File SCRIPT_FILE;
    private Process process;

    public PythonLauncher(String pythonCmd, File scriptFile) {
        this.PYTHON_CMD = pythonCmd;
        this.SCRIPT_FILE = scriptFile;
    }

    public PythonLauncher() {
        String os = System.getProperty("os.name").toLowerCase();
        String defaultCmd;
        if (os.contains("win")) {
            defaultCmd = "workers/.venv/Scripts/python.exe";
        } else {
            defaultCmd = "workers/.venv/bin/python";
        }
        this.PYTHON_CMD = defaultCmd;
        this.SCRIPT_FILE = new File("workers/harvest/__main__.py");
    }

    public synchronized void start() throws IOException {
        if (process != null && process.isAlive()) return;

        if (!SCRIPT_FILE.exists()) {
            throw new IOException("Python script not found: " + SCRIPT_FILE.getAbsolutePath());
        }

        // Run unbuffered (-u) so we see Python prints and errors immediately
        List<String> cmd = new ArrayList<>();
        cmd.add(PYTHON_CMD);
        cmd.add("-u");
        cmd.add(SCRIPT_FILE.getAbsolutePath());

        System.out.println("[PythonLauncher] Starting: " + String.join(" ", cmd));

        ProcessBuilder pb = new ProcessBuilder(cmd);
        // Make sure output is visible in the same console
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        // Ensure unbuffered in case -u is ignored by certain launchers
        pb.environment().put("PYTHONUNBUFFERED", "1");

        process = pb.start();

        // Fail fast if Python died immediately (missing deps, import error, etc.)
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ignored) {
        }
        if (!process.isAlive()) {
            int code;
            try {
                code = process.exitValue();
            } catch (IllegalThreadStateException e) {
                code = -1;
            }
            throw new IOException("Python process exited immediately with code " + code + ". Check console output above for errors.");
        }
    }

    /**
     * Attend que le process Python s’initialise (facultatif).
     */
    public void waitUntilReady(Duration timeout) {
        try {
            TimeUnit.MILLISECONDS.sleep(Math.min(timeout.toMillis(), 1200));
        } catch (InterruptedException ignored) {
        }
    }

    public synchronized void stop() {
        if (process != null) {
            process.destroy();
            try {
                if (!process.waitFor(1200, TimeUnit.MILLISECONDS)) process.destroyForcibly();
            } catch (InterruptedException ignored) {
            }
            process = null;
        }
    }

    public boolean isAlive() {
        return process != null && process.isAlive();
    }
}