package org.github.mitallast.taskflow.scheduler;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractLifecycleComponent;
import org.github.mitallast.taskflow.dag.*;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.github.mitallast.taskflow.operation.OperationStatus;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.github.mitallast.taskflow.common.Immutable.map;

/**
 * @todo refactoring:
 *
 * rewrite in functional style:
 * <pre>
 * type Command:
 *      TaskCommand
 *      DagCommand
 *      OperationCommand
 *
 * Scheduler:
 *      Command schedule(Dag, DagRun)
 * </pre>
 *
 * Required for better testing support.
 */
public class DagScheduler extends AbstractLifecycleComponent {

    private final DagPersistenceService persistenceService;
    private final TaskExecutor taskExecutor;
    private final ExecutorService executorService;

    @Inject
    public DagScheduler(Config config, DagPersistenceService persistenceService, TaskExecutor taskExecutor) {
        super(config, DagScheduler.class);
        this.persistenceService = persistenceService;
        this.taskExecutor = taskExecutor;

        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void schedule(long dagRunId) {
        logger.info("schedule {}", dagRunId);
        executorService.execute(() -> process(dagRunId));
    }

    private void process(long dagRunId) {
        Optional<DagRun> dagRunOpt = persistenceService.findDagRun(dagRunId);
        if (!dagRunOpt.isPresent()) {
            logger.warn("dag run {} not found", dagRunId);
            return;
        }
        DagRun dagRun = dagRunOpt.get();
        process(dagRun);
    }

    private void process(DagRun dagRun) {
        logger.info("process {}", dagRun.id());
        switch (dagRun.status()) {
            case PENDING:
                logger.info("dag pending: {}", dagRun);
                if (!persistenceService.startDagRun(dagRun.id())) {
                    logger.warn("error start dag run: {}", dagRun);
                    return;
                }
                schedule(dagRun.id());
                break;
            case RUNNING:
                logger.info("dag running: {}", dagRun);
                Optional<Dag> dagOpt = persistenceService.findDagById(dagRun.dagId());
                if (!dagOpt.isPresent()) {
                    logger.warn("dag not found: {}", dagRun);
                    markDagRunFailed(dagRun);
                    return;
                }
                Dag dag = dagOpt.get();
                try {
                    DagRunState dagRunState = new DagRunState(dag, dagRun);

                    if (dagRunState.hasLastRunCanceled()) {
                        logger.warn("found canceled tasks");

                        for (TaskRun taskRun : dagRun.tasks()) {
                            logger.info("task run {} status {}", taskRun.id(), taskRun.status());

                            if (taskRun.status() == TaskRunStatus.PENDING) {
                                logger.warn("cancel pending task: {}", taskRun.id());
                                persistenceService.markTaskRunCanceled(taskRun.id());
                                schedule(dagRun.id());
                                return;
                            }

                            if (taskRun.status() == TaskRunStatus.RUNNING) {
                                logger.info("await task run {}", taskRun.id());
                                return;
                            }
                        }
                        logger.warn("cancel dag: {}", dagRun.id());
                        persistenceService.markDagRunCanceled(dagRun.id());
                        schedule(dagRun.id());
                        return;
                    }

                    logger.info("check task run status");
                    for (TaskRun taskRun : dagRunState.lastTaskRuns()) {
                        logger.info("task run {} status {}", taskRun.id(), taskRun.status());

                        // @todo add retry policy
                        if (taskRun.status() == TaskRunStatus.FAILED) {
                            TaskRun retry = persistenceService.retry(taskRun);
                            logger.info("dag run has failed tasks, retry: {}", retry.id());
                            schedule(dagRun.id());
                            return;
                        }

                        if (taskRun.status() == TaskRunStatus.PENDING) {
                            if (dagRunState.taskRunDependsStatus(taskRun) == TaskRunStatus.SUCCESS) {
                                logger.info("execute run " + taskRun.id());
                                taskExecutor.schedule(taskRun);
                                schedule(dagRun.id());
                                return;
                            } else {
                                logger.info("await depends for task run {}: {}", taskRun.id(), map(dagRunState.depends(taskRun), Task::token));
                            }
                        }
                    }

                    if (dagRunState.hasUnfinished()) {
                        logger.info("await tasks");
                        return;
                    } else {
                        persistenceService.markDagRunSuccess(dagRun.id());
                        schedule(dagRun.id());
                        return;
                    }
                } catch (Exception e) {
                    logger.warn(e);
                    markDagRunFailed(dagRun);
                    return;
                }
            case SUCCESS:
            case CANCELED:
            case FAILED:
                logger.info("dag complete: {}", dagRun);
                break;
        }
    }

    protected void markDagRunFailed(DagRun dagRun) {
        for (TaskRun taskRun : dagRun.tasks()) {
            if (taskRun.status() == TaskRunStatus.PENDING) {
                logger.warn("cancel task: {}", taskRun);
                persistenceService.markTaskRunCanceled(taskRun.id());
                schedule(dagRun.id());
                return;
            }
        }
        persistenceService.markDagRunFailed(dagRun.id());
        schedule(dagRun.id());
    }

    protected void failureRunningTasks(DagRun dagRun) {
        for (TaskRun taskRun : dagRun.tasks()) {
            if (taskRun.status() == TaskRunStatus.RUNNING) {
                logger.warn("detected as running task: {}", taskRun);
                persistenceService.markTaskRunFailed(taskRun.id(), new OperationResult(OperationStatus.FAILED, "", "Detected as running task after start"));
            }
        }
    }

    @Override
    protected void doStart() throws IOException {
        for (DagRun dagRun : persistenceService.findPendingDagRuns()) {
            failureRunningTasks(dagRun);
            schedule(dagRun.id());
        }
    }

    @Override
    protected void doStop() throws IOException {

    }

    @Override
    protected void doClose() throws IOException {
        executorService.shutdown();
    }
}
