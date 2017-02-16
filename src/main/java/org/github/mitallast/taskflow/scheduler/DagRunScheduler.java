package org.github.mitallast.taskflow.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.github.mitallast.taskflow.dag.*;
import org.github.mitallast.taskflow.scheduler.command.*;

import static org.github.mitallast.taskflow.common.Immutable.map;

public class DagRunScheduler {

    private final static Logger logger = LogManager.getLogger();

    public Command schedule(Dag dag, DagRun dagRun) {
        logger.info("dag run {} status {}", dagRun.id(), dagRun.status());
        switch (dagRun.status()) {
            case PENDING:
                logger.info("dag pending: {}", dagRun.id());
                return new StartDagRunCommand(dagRun);
            case RUNNING:
                logger.info("dag running: {}", dagRun.id());
                DagRunState dagRunState = new DagRunState(dag, dagRun);

                if (dagRunState.hasLastRunCanceled()) {
                    logger.warn("found canceled tasks");

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
                    logger.warn("cancel dag: {}", dagRun.id());
                    return new CancelDagRunCommand(dagRun);
                }

                logger.info("check failed tasks");
                for (TaskRun taskRun : dagRunState.lastTaskRuns()) {
                    logger.debug("task run {} status {}", taskRun.id(), taskRun.status());

                    // @todo add retry policy
                    if (taskRun.status() == TaskRunStatus.FAILED) {
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
}
