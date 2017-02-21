package org.github.mitallast.taskflow.dag;

import com.google.common.collect.ImmutableList;
import org.github.mitallast.taskflow.operation.OperationResult;

import java.util.Optional;

public interface DagPersistenceService {

    Dag createDag(Dag dag);

    Dag updateDag(Dag dag);

    ImmutableList<Dag> findLatestDags();

    Optional<Dag> findDagById(long id);

    Optional<Dag> findDagByToken(String token);

    DagRun createDagRun(Dag dag);

    ImmutableList<DagRun> findDagRuns();

    ImmutableList<DagRun> findPendingDagRuns();

    ImmutableList<DagRun> findPendingDagRunsByDag(long dagId);

    Optional<DagRun> findDagRun(long id);

    boolean startDagRun(long id);

    boolean markDagRunSuccess(long id);

    boolean markDagRunFailed(long id);

    boolean markDagRunCanceled(long id);

    TaskRun retry(TaskRun taskRun);

    boolean startTaskRun(long id);

    boolean markTaskRunSuccess(long id, OperationResult operationResult);

    boolean markTaskRunFailed(long id, OperationResult operationResult);

    boolean markTaskRunCanceled(long id);

    ImmutableList<DagSchedule> findDagSchedules();

    ImmutableList<DagSchedule> findEnabledDagSchedules();

    Optional<DagSchedule> findDagSchedule(String token);

    boolean markDagScheduleEnabled(String token);

    boolean markDagScheduleDisabled(String token);

    boolean updateSchedule(DagSchedule dagSchedule);
}
