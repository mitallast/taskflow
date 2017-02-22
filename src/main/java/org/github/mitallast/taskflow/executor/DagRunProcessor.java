package org.github.mitallast.taskflow.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.github.mitallast.taskflow.dag.*;
import org.github.mitallast.taskflow.executor.command.*;

import static org.github.mitallast.taskflow.common.Immutable.map;

public class DagRunProcessor {

    private final static Logger logger = LogManager.getLogger();

    public Command process(DagRun dagRun) {
        Dag dag = dagRun.dag();
        logger.info("dag run {} status {}", dagRun.id(), dagRun.status());
        switch (dagRun.status()) {
            case PENDING:
                logger.info("dag pending: {}", dagRun.id());
                return new StartDagRunCommand(dagRun);
            case RUNNING:
                logger.info("dag running: {}", dagRun.id());
                DagRunState dagRunState = new DagRunState(dag, dagRun);

                if (dagRunState.hasFailedOutOfRetry()) {
                    logger.warn("found failed tasks out of retry");
                    CancelTaskRunCommand cancel = cancelUnfinishedTasks(dagRun);
                    if (cancel != null) {
                        return cancel;
                    }
                    logger.warn("failed dag run: {}", dagRun.id());
                    return new FailedDagRunCommand(dagRun);
                }

                if (dagRunState.hasLastRunCanceled()) {
                    logger.warn("found canceled tasks");
                    CancelTaskRunCommand cancel = cancelUnfinishedTasks(dagRun);
                    if (cancel != null) {
                        return cancel;
                    }
                    logger.warn("cancel dag run: {}", dagRun.id());
                    return new CancelDagRunCommand(dagRun);
                }

                logger.info("check failed tasks");
                for (TaskRun taskRun : dagRunState.lastTaskRuns()) {
                    logger.debug("task run {} status {}", taskRun.id(), taskRun.status());

                    // @todo add retry policy
                    if (taskRun.status() == TaskRunStatus.FAILED) {
                        Task task = dagRunState.task(taskRun);
                        if (dagRunState.taskRuns(task).size() >= task.retry()) {
                            logger.info("dag run has failed task run {} with out of retry, stop dag run", taskRun.id());
                            CancelTaskRunCommand cancel = cancelUnfinishedTasks(dagRun);
                            if (cancel != null) {
                                return cancel;
                            }
                            return new FailedDagRunCommand(dagRun);
                        }

                        logger.info("dag run has failed task, retry: {}", taskRun.id());
                        return new RetryTaskRunCommand(taskRun);
                    }
                }

                logger.info("check pending tasks");
                for (TaskRun taskRun : dagRunState.lastTaskRuns()) {
                    logger.debug("task run {} status {}", taskRun.id(), taskRun.status());

                    if (taskRun.status() == TaskRunStatus.PENDING) {
                        if (dagRunState.taskRunDependsStatus(taskRun) == TaskRunStatus.SUCCESS) {
                            logger.info("execute task run {}", taskRun.id());
                            return new ExecuteTaskRunCommand(taskRun);
                        } else {
                            logger.info("await depends for task run {}: {}", taskRun.id(), map(dagRunState.depends(taskRun), Task::token));
                        }
                    }
                }

                if (dagRunState.hasUnfinished()) {
                    logger.info("await tasks");
                    return new AwaitCommand(dagRun);
                } else {
                    logger.info("dag run success");
                    return new SuccessDagRunCommand(dagRun);
                }
            case SUCCESS:
            case CANCELED:
            case FAILED:
                logger.info("dag complete: {}", dagRun);
                return new CompleteDagRunCommand(dagRun);
            default:
                throw new IllegalStateException("Illegal dag run " + dagRun.id() + " status: " + dagRun.status());
        }
    }

    private CancelTaskRunCommand cancelUnfinishedTasks(DagRun dagRun) {
        // check for running or pending tasks
        for (TaskRun taskRun : dagRun.tasks()) {
            logger.info("task run {} status {}", taskRun.id(), taskRun.status());

            if (taskRun.status() == TaskRunStatus.PENDING) {
                logger.warn("cancel pending task run {}", taskRun.id());
                return new CancelTaskRunCommand(taskRun);
            }

            if (taskRun.status() == TaskRunStatus.RUNNING) {
                logger.info("cancel running task run {}", taskRun.id());
                return new CancelTaskRunCommand(taskRun);
            }
        }
        return null;
    }
}
