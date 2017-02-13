package org.github.mitallast.taskflow.dag;

import com.google.common.base.Preconditions;
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

    public TaskRun start() {
        Preconditions.checkArgument(status == TaskRunStatus.PENDING);
        Preconditions.checkArgument(startDate == null);
        Preconditions.checkArgument(finishDate == null);
        Preconditions.checkArgument(operationResult == null);
        return new TaskRun(id, dagId, taskId, dagRunId, createdDate, new DateTime(), null, TaskRunStatus.RUNNING, null);
    }

    public TaskRun success() {
        Preconditions.checkArgument(status == TaskRunStatus.RUNNING);
        Preconditions.checkNotNull(startDate);
        Preconditions.checkArgument(finishDate == null);
        return new TaskRun(id, dagId, taskId, dagRunId, createdDate, new DateTime(), new DateTime(), TaskRunStatus.SUCCESS, operationResult);
    }

    public TaskRun failure() {
        Preconditions.checkArgument(status == TaskRunStatus.RUNNING);
        Preconditions.checkNotNull(startDate);
        Preconditions.checkArgument(finishDate == null);
        return new TaskRun(id, dagId, taskId, dagRunId, createdDate, new DateTime(), new DateTime(), TaskRunStatus.FAILED, operationResult);
    }

    public TaskRun cancel() {
        Preconditions.checkArgument(status == TaskRunStatus.RUNNING);
        Preconditions.checkNotNull(startDate);
        Preconditions.checkArgument(finishDate == null);
        return new TaskRun(id, dagId, taskId, dagRunId, createdDate, new DateTime(), new DateTime(), TaskRunStatus.CANCELED, operationResult);
    }
}
