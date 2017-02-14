package org.github.mitallast.taskflow.dag;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.common.error.Errors;
import org.github.mitallast.taskflow.common.error.MaybeErrors;
import org.github.mitallast.taskflow.common.json.JsonService;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.github.mitallast.taskflow.operation.OperationService;
import org.github.mitallast.taskflow.scheduler.DagRunExecutor;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

public class DefaultDagService extends AbstractComponent implements DagService {

    private final DagPersistenceService persistenceService;
    private final DagRunExecutor dagRunExecutor;
    private final OperationService operationService;
    private final JsonService jsonService;

    @Inject
    public DefaultDagService(
        Config config,
        DagPersistenceService persistenceService,
        DagRunExecutor dagRunExecutor,
        OperationService operationService,
        JsonService jsonService
    ) {
        super(config, DagService.class);
        this.persistenceService = persistenceService;
        this.dagRunExecutor = dagRunExecutor;
        this.operationService = operationService;
        this.jsonService = jsonService;
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
        return validate(dag).maybe(() -> persistenceService.createDag(dag));
    }

    @Override
    public MaybeErrors<Dag> updateDag(Dag dag) {
        return validate(dag).maybe(() -> persistenceService.updateDag(dag));
    }

    @Override
    public DagRun createDagRun(Dag dag) {
        Preconditions.checkNotNull(dag);
        DagRun dagRun = persistenceService.createDagRun(dag);
        dagRunExecutor.schedule(dagRun.id());
        return dagRun;
    }

    @Override
    public boolean markTaskRunSuccess(TaskRun taskRun, OperationResult operationResult) {
        if (persistenceService.markTaskRunSuccess(taskRun.id(), operationResult)) {
            dagRunExecutor.schedule(taskRun.dagRunId());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean markTaskRunFailed(TaskRun taskRun, OperationResult operationResult) {
        if (persistenceService.markTaskRunFailed(taskRun.id(), operationResult)) {
            dagRunExecutor.schedule(taskRun.dagRunId());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean markTaskRunCanceled(TaskRun taskRun) {
        if (persistenceService.markTaskRunCanceled(taskRun.id())) {
            dagRunExecutor.schedule(taskRun.dagRunId());
            return true;
        } else {
            return false;
        }
    }
}
