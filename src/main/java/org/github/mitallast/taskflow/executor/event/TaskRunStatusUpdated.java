package org.github.mitallast.taskflow.executor.event;

import org.github.mitallast.taskflow.dag.TaskRun;
import org.github.mitallast.taskflow.dag.TaskRunStatus;
import org.github.mitallast.taskflow.operation.OperationStatus;
import org.joda.time.DateTime;

public class TaskRunStatusUpdated extends DagRunEvent {
    private final long taskRunId;
    private final DateTime startDate;
    private final DateTime finishDate;
    private final TaskRunStatus status;
    private final OperationStatus operationStatus;

    public TaskRunStatusUpdated(TaskRun taskRun) {
        taskRunId = taskRun.id();
        startDate = taskRun.startDate();
        finishDate = taskRun.finishDate();
        status = taskRun.status();
        if (taskRun.operationResult() != null) {
            operationStatus = taskRun.operationResult().status();
        } else {
            operationStatus = null;
        }
    }
}
