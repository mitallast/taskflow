package org.github.mitallast.taskflow.operation;

import com.google.common.base.Preconditions;

public class OperationResult {
    private final OperationStatus status;
    private final String output;

    public OperationResult(OperationStatus status, String output) {
        Preconditions.checkNotNull(status);
        Preconditions.checkNotNull(output);
        this.status = status;
        this.output = output;
    }

    public OperationStatus status() {
        return status;
    }

    public String output() {
        return output;
    }

    @Override
    public String toString() {
        return "OperationResult{" +
            "status=" + status +
            ", output='" + output + '\'' +
            '}';
    }
}
