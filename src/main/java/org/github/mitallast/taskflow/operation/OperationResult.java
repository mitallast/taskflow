package org.github.mitallast.taskflow.operation;

import com.google.common.base.Preconditions;

public class OperationResult {
    private final OperationStatus status;
    private final String stdout;
    private final String stderr;

    public OperationResult(OperationStatus status, String stdout, String stderr) {
        Preconditions.checkNotNull(status);
        Preconditions.checkNotNull(stdout);
        Preconditions.checkNotNull(stderr);
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
