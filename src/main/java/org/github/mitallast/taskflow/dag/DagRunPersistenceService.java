package org.github.mitallast.taskflow.dag;

import com.google.common.collect.ImmutableList;
import org.github.mitallast.taskflow.operation.OperationResult;

import java.util.Optional;

public interface DagRunPersistenceService {

    DagRun createDagRun(Dag dag);

    ImmutableList<DagRun> findDagRuns();

    ImmutableList<DagRun> findPendingDagRuns();

    ImmutableList<DagRun> findPendingDagRunsByDag(long dagId);

    Optional<DagRun> findDagRun(long id);

    boolean startDagRun(long id);

    boolean markDagRunSuccess(long id);

    boolean markDagRunFailed(long id);

    boolean markDagRunCanceled(long id);

    TaskRun retry(DagRun dagRun, TaskRun taskRun);

    boolean startTaskRun(long id);

    boolean markTaskRunSuccess(long id, OperationResult operationResult);

    boolean markTaskRunFailed(long id, OperationResult operationResult);

    boolean markTaskRunCanceled(long id);
}
