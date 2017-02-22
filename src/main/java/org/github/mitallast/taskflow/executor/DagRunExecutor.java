package org.github.mitallast.taskflow.executor;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractLifecycleComponent;
import org.github.mitallast.taskflow.dag.*;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.github.mitallast.taskflow.operation.OperationStatus;
import org.github.mitallast.taskflow.executor.command.*;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DagRunExecutor extends AbstractLifecycleComponent {

    private final DagRunPersistenceService persistenceService;
    private final DagRunProcessor dagRunScheduler;
    private final TaskRunExecutor taskRunExecutor;

    private final ExecutorService executorService;

    @Inject
    public DagRunExecutor(
        Config config,
        DagRunPersistenceService persistenceService,
        DagRunProcessor dagRunScheduler,
        TaskRunExecutor taskRunExecutor
    ) {
        super(config, DagRunExecutor.class);
        this.persistenceService = persistenceService;
        this.dagRunScheduler = dagRunScheduler;
        this.taskRunExecutor = taskRunExecutor;

        executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void cancel(long dagRunId) {
        logger.info("cancel {}", dagRunId);
        executorService.execute(() -> doCancel(dagRunId));
    }

    private void doCancel(long dagRunId) {
        Optional<DagRun> dagRunOpt = persistenceService.findDagRun(dagRunId);
        if (!dagRunOpt.isPresent()) {
            logger.warn("dag run {} not found", dagRunId);
            return;
        }
        DagRun dagRun = dagRunOpt.get();

        cancel(dagRun);
    }

    private void cancel(final DagRun dagRun) {
        if (dagRun.status() == DagRunStatus.PENDING || dagRun.status() == DagRunStatus.RUNNING) {
            for (TaskRun taskRun : dagRun.tasks()) {
                if (taskRun.status() == TaskRunStatus.PENDING) {
                    handle(new CancelTaskRunCommand(taskRun));
                    return;
                }
                if (taskRun.status() == TaskRunStatus.RUNNING) {
                    handle(new CancelTaskRunCommand(taskRun));
                    return;
                }
            }
            handle(new CancelDagRunCommand(dagRun));
        } else {
            logger.warn("dag run {} not in progress", dagRun);
        }
    }

    public void schedule(long dagRunId) {
        logger.info("process {}", dagRunId);
        executorService.execute(() -> process(dagRunId));
    }

    private void process(long dagRunId) {
        try {
            logger.info("process {}", dagRunId);
            Optional<DagRun> dagRunOpt = persistenceService.findDagRun(dagRunId);
            if (!dagRunOpt.isPresent()) {
                logger.warn("dag run {} not found", dagRunId);
                return;
            }
            DagRun dagRun = dagRunOpt.get();
            Dag dag = dagRun.dag();

            process(dag, dagRun);
        } catch (Exception e) {
            logger.warn("unexpected exception", e);
        }
    }

    private void process(final Dag dag, final DagRun dagRun) {
        final Command cmd = dagRunScheduler.process(dag, dagRun);
        if (cmd instanceof DagRunCommand) {
            handle((DagRunCommand) cmd);
        } else if (cmd instanceof TaskRunCommand) {
            handle((TaskRunCommand) cmd);
        } else {
            logger.warn("unexpected command: {}", cmd);
        }
    }

    private void handle(DagRunCommand cmd) {
        final DagRun dagRun = cmd.dagRun();
        if (cmd instanceof StartDagRunCommand) {
            logger.info("start dag run {}", dagRun.id());
            persistenceService.startDagRun(dagRun.id());
            schedule(dagRun.id());

        } else if (cmd instanceof AwaitCommand) {
            logger.info("await dag run {}", dagRun.id());

        } else if (cmd instanceof FailedDagRunCommand) {
            logger.info("failed dag run {}", dagRun.id());
            persistenceService.markDagRunFailed(dagRun.id());
            schedule(dagRun.id());

        } else if (cmd instanceof SuccessDagRunCommand) {
            logger.info("sucess dag run {}", dagRun.id());
            persistenceService.markDagRunSuccess(dagRun.id());
            schedule(dagRun.id());

        } else if (cmd instanceof CancelDagRunCommand) {
            logger.info("cancel dag run {}", dagRun.id());
            persistenceService.markDagRunCanceled(dagRun.id());
            schedule(dagRun.id());

        } else if (cmd instanceof CompleteDagRunCommand) {
            logger.info("complete dag run {}", dagRun.id());

        } else {
            logger.warn("unexpected dag run command: {}", cmd);
        }
    }

    private void handle(TaskRunCommand cmd) {
        final TaskRun taskRun = cmd.taskRun();
        if (cmd instanceof ExecuteTaskRunCommand) {
            logger.warn("execute task run: {}", taskRun.id());
            persistenceService.startTaskRun(taskRun.id());
            taskRunExecutor.schedule(taskRun);
            schedule(taskRun.dagRunId());

        } else if (cmd instanceof RetryTaskRunCommand) {
            logger.warn("retry task run: {}", taskRun.id());
            persistenceService.retry(taskRun);
            schedule(taskRun.dagRunId());

        } else if (cmd instanceof CancelTaskRunCommand && taskRun.status() == TaskRunStatus.PENDING) {
            logger.warn("cancel pending task run: {}", taskRun.id());
            persistenceService.markDagRunCanceled(taskRun.id());
            schedule(taskRun.dagRunId());

        } else if (cmd instanceof CancelTaskRunCommand && taskRun.status() == TaskRunStatus.RUNNING) {
            logger.warn("cancel running task run: {}", taskRun.id());
            taskRunExecutor.cancel(taskRun);
        } else {
            logger.warn("unexpected task run command: {}", cmd);
        }
    }

    private void failureRunningTasks(DagRun dagRun) {
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

    }
}
