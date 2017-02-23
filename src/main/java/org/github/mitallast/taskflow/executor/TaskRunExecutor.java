package org.github.mitallast.taskflow.executor;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.dag.DagRun;
import org.github.mitallast.taskflow.dag.DagService;
import org.github.mitallast.taskflow.dag.Task;
import org.github.mitallast.taskflow.dag.TaskRun;
import org.github.mitallast.taskflow.operation.Operation;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.github.mitallast.taskflow.operation.OperationService;
import org.github.mitallast.taskflow.operation.OperationStatus;

import java.util.concurrent.*;

public class TaskRunExecutor extends AbstractComponent {

    private final DagService dagService;
    private final OperationService operationService;
    private final ExecutorService executorService;

    private final ConcurrentMap<TaskRun, Future<?>> futures;

    @Inject
    public TaskRunExecutor(Config config, DagService dagService, OperationService operationService) {
        super(config, TaskRunExecutor.class);
        this.dagService = dagService;
        this.operationService = operationService;
        this.executorService = Executors.newCachedThreadPool();
        futures = new ConcurrentHashMap<>();
    }

    public void cancel(TaskRun taskRun) {
        Future future = futures.get(taskRun);
        if (future != null) {
            if (future.cancel(true)) {
                logger.info("task run {} canceled", taskRun.id());
            } else {
                logger.warn("task run {} does not canceled", taskRun.id());
            }
        } else {
            logger.warn("task run {} not running", taskRun.id());
        }
    }

    public void schedule(DagRun dagRun, TaskRun taskRun) {
        futures.computeIfAbsent(taskRun, t -> executorService.submit(() -> execute(dagRun, taskRun)));
    }

    private void execute(DagRun dagRun, TaskRun taskRun) {
        try {
            Task task = taskRun.task();

            Operation operation = operationService.operation(task.operation());
            if (operation == null) {
                logger.warn("task run {} operation {} not found", taskRun.id(), task.operation());
                dagService.markTaskRunFailed(dagRun, taskRun, new OperationResult(OperationStatus.FAILED, "", "operation not found"));
                return;
            }

            OperationResult operationResult = operation.run(task.command());
            switch (operationResult.status()) {
                case SUCCESS:
                    logger.info("task run {} operation success: {}", taskRun.id(), operationResult);
                    dagService.markTaskRunSuccess(dagRun, taskRun, operationResult);
                    break;
                case FAILED:
                    logger.error("task run {} operation failed: {}", taskRun.id(), operationResult);
                    dagService.markTaskRunFailed(dagRun, taskRun, operationResult);
                    break;
            }
        } catch (InterruptedException e) {
            logger.warn("task run {} canceled", taskRun.id(), e);
            dagService.markTaskRunCanceled(dagRun, taskRun);
        } catch (Exception e) {
            logger.warn("task run {} failed", taskRun.id(), e);
            dagService.markTaskRunFailed(dagRun, taskRun, new OperationResult(OperationStatus.FAILED, "", e.toString()));
        } finally {
            // cleanup to prevent memory leak
            futures.remove(taskRun);
        }
    }
}
