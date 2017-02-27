package org.github.mitallast.taskflow.executor.event;

public class TaskRunNewOutputLine extends DagRunEvent {
    private final long taskRunId;
    private final int offset;
    private final String line;

    public TaskRunNewOutputLine(long taskRunId, int offset, String line) {
        this.taskRunId = taskRunId;
        this.offset = offset;
        this.line = line;
    }
}
