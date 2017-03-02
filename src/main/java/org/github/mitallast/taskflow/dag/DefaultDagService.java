package org.github.mitallast.taskflow.dag;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.EventBus;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.common.error.Errors;
import org.github.mitallast.taskflow.common.error.MaybeErrors;
import org.github.mitallast.taskflow.common.json.JsonService;
import org.github.mitallast.taskflow.executor.DagRunExecutor;
import org.github.mitallast.taskflow.executor.event.DagRunEvent;
import org.github.mitallast.taskflow.executor.event.DagRunStatusUpdated;
import org.github.mitallast.taskflow.executor.event.DagRunUpdated;
import org.github.mitallast.taskflow.executor.event.TaskRunStatusUpdated;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.github.mitallast.taskflow.operation.OperationService;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

public class DefaultDagService extends AbstractComponent implements DagService {

    private final DagPersistenceService dagPersistence;
    private final DagRunPersistenceService dagRunPersistence;
    private final DagRunExecutor dagRunExecutor;
    private final DagRunNotificationService notificationService;
    private final OperationService operationService;
    private final JsonService jsonService;
    private final EventBus<DagRunEvent> eventBus;

    @Inject
    public DefaultDagService(
        Config config,
        DagPersistenceService dagPersistence,
        DagRunPersistenceService dagRunPersistence,
        DagRunExecutor dagRunExecutor,
        OperationService operationService,
        JsonService jsonService,
        DagRunNotificationService notificationService,
        EventBus<DagRunEvent> eventBus
    ) {
        super(config, DagService.class);
        this.dagPersistence = dagPersistence;
        this.dagRunPersistence = dagRunPersistence;
        this.dagRunExecutor = dagRunExecutor;
        this.operationService = operationService;
        this.jsonService = jsonService;
        this.notificationService = notificationService;
        this.eventBus = eventBus;
    }

    @Override
    public Errors validate(Dag dag) {
        logger.info("validate dag: {}", dag);
        final Errors builder = new Errors();
        builder.required(dag.token()).accept("token", "required");
        builder.required(dag.tasks()).accept("tasks_list", "required");

        if (dag.tasks() != null && !dag.tasks().isEmpty()) {
            ImmutableList<Task> tasks = dag.tasks();
            ImmutableSet<String> tokens = ImmutableSet.copyOf(tasks.stream().map(Task::token).iterator());

            builder.on(tokens.size() < tasks.size()).accept("tasks_list", "contains not unique tokens");

            try {
                // validate is this graph acyclic
                DirectedAcyclicGraph<String, DefaultEdge> graph = new DirectedAcyclicGraph<>(DefaultEdge.class);
                for (Task task : tasks) {
                    graph.addVertex(task.token());
                }
                for (Task task : tasks) {
                    for (String depends : task.depends()) {
                        graph.addEdge(depends, task.token());
                    }
                }
            } catch (IllegalArgumentException e) {
                if (e.getCause() != null) {
                    if (e.getCause() instanceof DirectedAcyclicGraph.CycleFoundException) {
                        logger.warn(e.getCause());
                        builder.error("tasks_list", "invalid dag: cycle found");
                    } else {
                        logger.warn(e.getCause());
                        builder.error("tasks_list", e.getCause().getMessage());
                    }
                } else {
                    logger.warn(e);
                    builder.error("tasks_list", e.getMessage());
                }
            }

            for (int i = 0; i < tasks.size(); i++) {
                Errors taskBuilder = builder.builder("tasks").builder(i);
                Task task = tasks.get(i);
                taskBuilder.required(task.token()).accept("token", "required");

                taskBuilder.required(task.operation()).accept("operation", "required");
                if (!Strings.isNullOrEmpty(task.operation())) {
                    taskBuilder.not(operationService.contains(task.operation())).accept("operation", "unexpected operation");
                }

                if (task.depends() != null && !task.depends().isEmpty()) {
                    for (String depends : task.depends()) {
                        taskBuilder.required(depends).accept("depends", "empty token");
                        taskBuilder.not(tokens.contains(depends)).accept("depends", "undefined token");
                        taskBuilder.on(depends.equals(task.token())).accept("depends", "self-loop detected");
                    }
                }

                taskBuilder.notNull(task.command()).accept("command", "required");
                if (task.command() != null) {
                    Errors commandBuilder = taskBuilder.builder("command");
                    commandBuilder.notNull(task.command().config()).accept("config", "required");
                    commandBuilder.notNull(task.command().environment()).accept("environment", "required");
                }
            }
        }

        if (!builder.valid()) {
            logger.warn("dag errors: {}", jsonService.serialize(builder));
        } else {
            logger.info("dag is valid");
        }

        return builder;
    }

    @Override
    public MaybeErrors<Dag> createDag(Dag dag) {
        return validate(dag).maybe(() -> dagPersistence.createDag(dag));
    }

    @Override
    public MaybeErrors<Dag> updateDag(Dag dag) {
        return validate(dag).maybe(() -> dagPersistence.updateDag(dag));
    }

    @Override
    public DagRun createDagRun(Dag dag) {
        Preconditions.checkNotNull(dag);
        DagRun dagRun = dagRunPersistence.createDagRun(dag);
        dagRunExecutor.schedule(dagRun.id());
        return dagRun;
    }

    @Override
    public boolean startDagRun(DagRun dagRun) {
        Preconditions.checkNotNull(dagRun);
        if (dagRunPersistence.startDagRun(dagRun.id())) {
            triggerStatusUpdated(dagRun);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean markDagRunSuccess(DagRun dagRun) {
        Preconditions.checkNotNull(dagRun);
        if (dagRunPersistence.markDagRunSuccess(dagRun.id())) {
            triggerStatusUpdated(dagRun);
            triggerRemove(dagRun);
            notificationService.sendDagSuccess(dagRun);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean markDagRunFailed(DagRun dagRun) {
        Preconditions.checkNotNull(dagRun);
        if (dagRunPersistence.markDagRunFailed(dagRun.id())) {
            triggerStatusUpdated(dagRun);
            triggerRemove(dagRun);
            notificationService.sendDagFailed(dagRun);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean markDagRunCanceled(DagRun dagRun) {
        Preconditions.checkNotNull(dagRun);
        if (dagRunPersistence.markDagRunCanceled(dagRun.id())) {
            triggerStatusUpdated(dagRun);
            triggerRemove(dagRun);
            notificationService.sendDagCanceled(dagRun);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public TaskRun retry(DagRun dagRun, TaskRun taskRun) {
        try {
            return dagRunPersistence.retry(dagRun, taskRun);
        } finally {
            trigger(dagRun);
        }
    }

    @Override
    public boolean startTaskRun(DagRun dagRun, TaskRun taskRun) {
        try {
            return dagRunPersistence.startTaskRun(taskRun.id());
        } finally {
            triggerStatusUpdated(dagRun, taskRun);
        }
    }

    @Override
    public boolean markTaskRunSuccess(DagRun dagRun, TaskRun taskRun, OperationResult operationResult) {
        try {
            return dagRunPersistence.markTaskRunSuccess(taskRun.id(), operationResult);
        } finally {
            triggerStatusUpdated(dagRun, taskRun);
        }
    }

    @Override
    public boolean markTaskRunFailed(DagRun dagRun, TaskRun taskRun, OperationResult operationResult) {
        try {
            return dagRunPersistence.markTaskRunFailed(taskRun.id(), operationResult);
        } finally {
            triggerStatusUpdated(dagRun, taskRun);
            notificationService.sendTaskFailed(dagRun, taskRun, operationResult);
        }
    }

    @Override
    public boolean markTaskRunCanceled(DagRun dagRun, TaskRun taskRun) {
        try {
            return dagRunPersistence.markTaskRunCanceled(taskRun.id());
        } finally {
            triggerStatusUpdated(dagRun, taskRun);
        }
    }

    private void triggerRemove(DagRun dagRun) {
        eventBus.remove(channel(dagRun));
    }

    private void trigger(DagRun dagRun) {
        try {
            DagRun updated = dagRunPersistence.findDagRun(dagRun.id()).orElseThrow(IllegalArgumentException::new);
            eventBus.trigger(channel(dagRun), new DagRunUpdated(updated));
        } catch (Throwable e) {
            logger.warn(e);
        }
    }

    private void triggerStatusUpdated(DagRun dagRun) {
        try {
            DagRun updated = dagRunPersistence.findDagRun(dagRun.id()).orElseThrow(IllegalArgumentException::new);
            eventBus.trigger(channel(dagRun), new DagRunStatusUpdated(updated));
        } catch (Throwable e) {
            logger.warn(e);
        }
    }

    private void triggerStatusUpdated(DagRun dagRun, TaskRun taskRun) {
        try {
            DagRun updated = dagRunPersistence.findDagRun(dagRun.id()).orElseThrow(IllegalArgumentException::new);
            TaskRun updatedTask = updated.tasks().stream().filter(t -> t.id() == taskRun.id()).findFirst().orElseThrow(IllegalArgumentException::new);
            eventBus.trigger(channel(dagRun), new TaskRunStatusUpdated(updatedTask));
        } catch (Throwable e) {
            logger.warn(e);
        }
    }

    private String channel(DagRun dagRun) {
        return "dag/run/" + dagRun.id();
    }
}
