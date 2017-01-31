package org.github.mitallast.taskflow.operation;

public class OperationResult {
    private final OperationStatus status;
    private final String stdout;
    private final String stderr;

    public OperationResult(OperationStatus status, String stdout, String stderr) {
        this.status = status;
        this.stdout = stdout;
        this.stderr = stderr;
    }

    public OperationStatus status() {
        return status;
    }

    public String stdout() {
        return stdout;
    }

    public String stderr() {
        return stderr;
    }

    @Override
    public String toString() {
        return "OperationResult{" +
            "status=" + status +
            ", stdout='" + stdout + '\'' +
            ", stderr='" + stderr + '\'' +
            '}';
    }
}
