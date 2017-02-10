package org.github.mitallast.taskflow.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
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
                }
                schedule(dagRun.id());
                break;
            case RUNNING:
                logger.info("dag running: {}", dagRun);
                try {
                    processRunning(dagRun);
                } catch (Exception e) {
                    logger.warn(e);
                    persistenceService.markDagRunFailed(dagRun.id());
                }
                break;
            case SUCCESS:
            case CANCELED:
            case FAILED:
                logger.info("dag complete: {}", dagRun);
                break;
        }
    }

    private void processRunning(DagRun dagRun) {
        Optional<Dag> dagOpt = persistenceService.findDagById(dagRun.dagId());
        if (!dagOpt.isPresent()) {
            logger.warn("dag not found: {}", dagRun);
            persistenceService.markDagRunFailed(dagRun.id());
            return;
        }
        Dag dag = dagOpt.get();

        ImmutableMap<Long, Task> idTaskMap = idTaskMap(dag);
        ImmutableMap<String, Task> tokenTaskMap = tokenTaskMap(dag);

        ImmutableMultimap<Long, TaskRun> idTaskRunMap = idTaskRunMap(dagRun);

        boolean hasPending = false;
        boolean hasRunning = false;
        boolean hasSuccess = false;
        boolean hasFailed = false;
        boolean hasCanceled = false;

        for (TaskRun taskRun : dagRun.tasks()) {
            switch (taskRun.status()) {
                case PENDING:
                    hasPending = true;
                    logger.info("task pending: {}", taskRun);
                    Task task = idTaskMap.get(taskRun.taskId());
                    if (task == null) {
                        logger.warn("task not found: {}", taskRun);
                        throw new IllegalArgumentException("task not found: " + taskRun);
                    }

                    switch (checkDepends(task, tokenTaskMap, idTaskRunMap)) {
                        case PENDING:
                            logger.info("task run depends pending");
                            break;
                        case RUNNING:
                            logger.info("task run depends running");
                            break;
                        case SUCCESS:
                            logger.info("task run depends success");
                            taskExecutor.schedule(taskRun);
                            break;
                        case FAILED:
                            logger.info("task run depends tailed");
                            dagService.markTaskRunCanceled(taskRun);
                            return;
                        case CANCELED:
                            logger.info("task run depends canceled");
                            dagService.markTaskRunCanceled(taskRun);
                            return;
                    }
                    break;
                case RUNNING:
                    hasRunning = true;
                    logger.info("task running: {}", taskRun);
                    break;
                case SUCCESS:
                    hasSuccess = true;
                case FAILED:
                    hasFailed = true;
                case CANCELED:
                    hasCanceled = true;
                    logger.info("task done: {}", taskRun);
                    break;
            }
        }

        if (hasPending || hasRunning) {
            logger.info("dag run has not finished tasks: {}", dagRun);
        } else {
            persistenceService.markDagRunSuccess(dagRun.id());
        }
    }

    private TaskRunStatus checkDepends(Task task, ImmutableMap<String, Task> tokenTaskMap, ImmutableMultimap<Long, TaskRun> idTaskRunMap) {
        ImmutableList<Task> depends = depends(tokenTaskMap, task);

        boolean hasPending = false;
        boolean hasRunning = false;
        boolean hasSuccess = false;
        TaskRunStatus lastFinishedStatus = null;

        for (Task dependTask : depends) {
            switch (checkDepend(dependTask, idTaskRunMap)) {
                case PENDING:
                    hasPending = true;
                    break;
                case RUNNING:
                    hasRunning = true;
                    break;
                case SUCCESS:
                    hasSuccess = true;
                    break;
                case FAILED:
                    lastFinishedStatus = TaskRunStatus.FAILED;
                    break;
                case CANCELED:
                    lastFinishedStatus = TaskRunStatus.CANCELED;
                    break;
            }
        }
        if (hasPending) {
            return TaskRunStatus.PENDING;
        }
        if (hasRunning) {
            return TaskRunStatus.RUNNING;
        }
        if (hasSuccess) {
            return TaskRunStatus.SUCCESS;
        }
        if (lastFinishedStatus != null) {
            return lastFinishedStatus;
        }
        return TaskRunStatus.SUCCESS;
    }

    private TaskRunStatus checkDepend(Task dependTask, ImmutableMultimap<Long, TaskRun> idTaskRunMap) {
        boolean hasPending = false;
        boolean hasRunning = false;
        boolean hasSuccess = false;
        TaskRunStatus lastFinishedStatus = null;
        for (TaskRun run : idTaskRunMap.get(dependTask.id())) {
            switch (run.status()) {
                case PENDING:
                    hasPending = true;
                    break;
                case RUNNING:
                    hasRunning = true;
                    break;
                case SUCCESS:
                    hasSuccess = true;
                    break;
                case FAILED:
                    lastFinishedStatus = TaskRunStatus.FAILED;
                    break;
                case CANCELED:
                    lastFinishedStatus = TaskRunStatus.CANCELED;
                    break;
            }
        }
        if (hasPending) {
            return TaskRunStatus.PENDING;
        }
        if (hasRunning) {
            return TaskRunStatus.RUNNING;
        }
        if (hasSuccess) {
            return TaskRunStatus.SUCCESS;
        }
        if (lastFinishedStatus != null) {
            return lastFinishedStatus;
        }
        return TaskRunStatus.SUCCESS;
    }

    private static ImmutableList<Task> depends(ImmutableMap<String, Task> tokenTaskMap, Task task) {
        ImmutableList.Builder<Task> builder = ImmutableList.builder();
        for (String dependsToken : task.depends()) {
            Task dependsTask = tokenTaskMap.get(dependsToken);
            if (dependsTask == null) {
                throw new IllegalArgumentException("task not found: " + dependsToken);
            }
            builder.add(dependsTask);
        }
        return builder.build();
    }

    private static ImmutableMultimap<Long, TaskRun> idTaskRunMap(DagRun dagRun) {
        ImmutableMultimap.Builder<Long, TaskRun> builder = ImmutableMultimap.builder();
        for (TaskRun task : dagRun.tasks()) {
            builder.put(task.taskId(), task);
        }
        return builder.build();
    }

    private static ImmutableMap<String, Task> tokenTaskMap(Dag dag) {
        ImmutableMap.Builder<String, Task> builder = ImmutableMap.builder();
        for (Task task : dag.tasks()) {
            builder.put(task.token(), task);
        }
        return builder.build();
    }

    private static ImmutableMap<Long, Task> idTaskMap(Dag dag) {
        ImmutableMap.Builder<Long, Task> builder = ImmutableMap.builder();
        for (Task task : dag.tasks()) {
            builder.put(task.id(), task);
        }
        return builder.build();
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
