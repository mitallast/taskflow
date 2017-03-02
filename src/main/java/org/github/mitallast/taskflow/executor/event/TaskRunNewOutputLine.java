package org.github.mitallast.taskflow.executor.event;

public class TaskRunNewOutputLine extends DagRunEvent {
    private final long taskRunId;
    private final String line;

    public TaskRunNewOutputLine(long taskRunId, String line) {
        this.taskRunId = taskRunId;
        this.line = line;
    }
}
