package org.github.mitallast.taskflow.scheduler;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractLifecycleComponent;
import org.github.mitallast.taskflow.dag.*;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DagScheduler extends AbstractLifecycleComponent {

    private final DagPersistenceService persistenceService;
    private final DagService dagService;
    private final TaskExecutor taskExecutor;
    private final ExecutorService executorService;

    @Inject
    public DagScheduler(Config config, DagPersistenceService persistenceService, DagService dagService, TaskExecutor taskExecutor) {
        super(config, DagScheduler.class);
        this.persistenceService = persistenceService;
        this.dagService = dagService;
        this.taskExecutor = taskExecutor;

        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void schedule(long dagRunId) {
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
                    persistenceService.markDagRunFailed(dagRun.id());
                    return;
                }
                Dag dag = dagOpt.get();
                try {
                    DagRunState dagRunState = new DagRunState(dag, dagRun);
                    switch (dagRunState.dagRunDependsStatus()) {
                        case PENDING:
                        case RUNNING:
                            for (Task task : dag.tasks()) {
                                TaskRun taskRun = dagRunState.taskRun(task);

                                // does not have failed tasks on last run
                                if (taskRun.status() == TaskRunStatus.PENDING) {
                                    if (dagRunState.taskRunDependsStatus(task) == TaskRunStatus.SUCCESS) {
                                        logger.info("execute run " + taskRun.id());
                                        taskExecutor.schedule(taskRun);
                                    } else {
                                        logger.info("await depends for run " + taskRun.id());
                                    }
                                } else {
                                    logger.info("task run status " + taskRun.status());
                                }
                            }

                            break;
                        case FAILED:
                            persistenceService.markDagRunFailed(dagRun.id());
                            schedule(dagRun.id());
                            break;
                        case SUCCESS:
                            persistenceService.markDagRunSuccess(dagRun.id());
                            schedule(dagRun.id());
                            break;
                    }
                } catch (Exception e) {
                    logger.warn(e);
                    persistenceService.markDagRunFailed(dagRun.id());
                    schedule(dagRun.id());
                }
                break;
            case SUCCESS:
            case CANCELED:
            case FAILED:
                logger.info("dag complete: {}", dagRun);
                break;
        }
    }

    @Override
    protected void doStart() throws IOException {
        for (DagRun dagRun : persistenceService.findPendingDagRuns()) {
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
