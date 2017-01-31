package org.github.mitallast.taskflow.dag;

import com.google.common.collect.ImmutableList;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.joda.time.DateTime;

public class TaskRun {
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

    public long getId() {
        return id;
    }

    public long getDagId() {
        return dagId;
    }

    public long getTaskId() {
        return taskId;
    }

    public long getDagRunId() {
        return dagRunId;
    }

    public DateTime getCreatedDate() {
        return createdDate;
    }

    public DateTime getStartDate() {
        return startDate;
    }

    public DateTime getFinishDate() {
        return finishDate;
    }

    public TaskRunStatus getStatus() {
        return status;
    }

    public OperationResult getOperationResult() {
        return operationResult;
    }
}
