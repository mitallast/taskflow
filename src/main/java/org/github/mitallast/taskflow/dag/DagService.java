package org.github.mitallast.taskflow.dag;

import org.github.mitallast.taskflow.common.error.Errors;
import org.github.mitallast.taskflow.common.error.MaybeErrors;
import org.github.mitallast.taskflow.operation.OperationResult;

public interface DagService {

    Errors validate(Dag dag);

    MaybeErrors<Dag> createDag(Dag dag);

    MaybeErrors<Dag> updateDag(Dag dag);

    DagRun createDagRun(Dag dag);

    boolean markTaskRunSuccess(DagRun dagRun, TaskRun taskRun, OperationResult operationResult);

    boolean markTaskRunFailed(DagRun dagRun, TaskRun taskRun, OperationResult operationResult);

    boolean markTaskRunCanceled(DagRun dagRun, TaskRun taskRun);
}
