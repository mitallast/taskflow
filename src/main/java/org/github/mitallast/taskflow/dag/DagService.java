package org.github.mitallast.taskflow.dag;

import org.github.mitallast.taskflow.common.error.Errors;
import org.github.mitallast.taskflow.common.error.MaybeErrors;
import org.github.mitallast.taskflow.operation.OperationResult;

public interface DagService {

    Errors validate(Dag dag);

    MaybeErrors<Dag> createDag(Dag dag);

    MaybeErrors<Dag> updateDag(Dag dag);

    DagRun createDagRun(Dag dag);

    boolean startDagRun(DagRun dagRun);

    boolean markDagRunSuccess(DagRun dagRun);

    boolean markDagRunFailed(DagRun dagRun);

    boolean markDagRunCanceled(DagRun dagRun);

    TaskRun retry(DagRun dagRun, TaskRun taskRun);

    boolean startTaskRun(DagRun dagRun, TaskRun taskRun);

    boolean markTaskRunSuccess(DagRun dagRun, TaskRun taskRun, OperationResult operationResult);

    boolean markTaskRunFailed(DagRun dagRun, TaskRun taskRun, OperationResult operationResult);

    boolean markTaskRunCanceled(DagRun dagRun, TaskRun taskRun);
}
