package org.github.mitallast.taskflow.dag;

import org.github.mitallast.taskflow.operation.OperationResult;
import org.joda.time.DateTime;

public class TaskRun implements Comparable<TaskRun> {
    private final long id;
    private final long dagId;
    private final long taskId;
    private final long dagRunId;
    private final DateTime createdDate;
    private final DateTime startDate;
    private final DateTime finishDate;
    private final TaskRunStatus status;
    private final OperationResult operationResult;

    public TaskRun(long id, long dagId, long taskId, long dagRunId, DateTime createdDate, DateTime startDate, DateTime finishDate, TaskRunStatus status, OperationResult operationResult) {
        this.id = id;
        this.dagId = dagId;
        this.taskId = taskId;
        this.dagRunId = dagRunId;
        this.createdDate = createdDate;
        this.startDate = startDate;
        this.finishDate = finishDate;
        this.status = status;
        this.operationResult = operationResult;
    }

    public long id() {
        return id;
    }

    public long dagId() {
        return dagId;
    }

    public long taskId() {
        return taskId;
    }

    public long dagRunId() {
        return dagRunId;
    }

    public DateTime createdDate() {
        return createdDate;
    }

    public DateTime startDate() {
        return startDate;
    }

    public DateTime finishDate() {
        return finishDate;
    }

    public TaskRunStatus status() {
        return status;
    }

    public OperationResult operationResult() {
        return operationResult;
    }

    @Override
    public int compareTo(TaskRun other) {
        return Long.compare(this.id, other.id);
    }
}
