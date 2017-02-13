package org.github.mitallast.taskflow.scheduler;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.dag.*;
import org.github.mitallast.taskflow.operation.Operation;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.github.mitallast.taskflow.operation.OperationService;
import org.github.mitallast.taskflow.operation.OperationStatus;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskRunExecutor extends AbstractComponent {

    private final DagPersistenceService persistenceService;
    private final DagService dagService;
    private final OperationService operationService;
    private final ExecutorService executorService;

    @Inject
    public TaskRunExecutor(Config config, DagPersistenceService persistenceService, DagService dagService, OperationService operationService) {
        super(config, TaskRunExecutor.class);
        this.persistenceService = persistenceService;
        this.dagService = dagService;
        this.operationService = operationService;
        this.executorService = Executors.newCachedThreadPool();
    }

    public void schedule(TaskRun taskRun) {
        executorService.execute(() -> execute(taskRun));
    }

    private void execute(TaskRun taskRun) {
        Optional<Dag> dagOpt = persistenceService.findDagById(taskRun.dagId());
        if (!dagOpt.isPresent()) {
            logger.warn("dag not found: {}", taskRun);
            dagService.markTaskRunFailed(taskRun, new OperationResult(OperationStatus.FAILED, "", "dag not found"));
            return;
        }
        Dag dag = dagOpt.get();

        Optional<Task> taskOpt = dag.tasks().stream().filter(task -> task.id() == taskRun.taskId()).findFirst();
        if (!taskOpt.isPresent()) {
            logger.warn("task not found: {}", taskRun);
            dagService.markTaskRunFailed(taskRun, new OperationResult(OperationStatus.FAILED, "", "task not found"));
            return;
        }
        Task task = taskOpt.get();

        Operation operation = operationService.operation(task.operation());
        if (operation == null) {
            logger.warn("operation not found: {}", taskRun);
            dagService.markTaskRunFailed(taskRun, new OperationResult(OperationStatus.FAILED, "", "operation not found"));
            return;
        }

        try {
            OperationResult operationResult = operation.run(task.command());
            switch (operationResult.status()) {
                case SUCCESS:
                    logger.info("operation success: {}", operationResult);
                    dagService.markTaskRunSuccess(taskRun, operationResult);
                    break;
                case FAILED:
                    logger.error("operation failed: {}", operationResult);
                    dagService.markTaskRunFailed(taskRun, operationResult);
                    break;
            }
        } catch (IOException e) {
            logger.warn("error on running task: {}", task, e);
            dagService.markTaskRunFailed(taskRun, new OperationResult(OperationStatus.FAILED, "", e.toString()));
        }
    }
}
