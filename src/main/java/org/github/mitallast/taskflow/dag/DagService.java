package org.github.mitallast.taskflow.dag;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.common.error.Errors;
import org.github.mitallast.taskflow.common.error.MaybeErrors;
import org.github.mitallast.taskflow.common.json.JsonService;
import org.github.mitallast.taskflow.operation.OperationService;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

public class DagService extends AbstractComponent {

    private final DagPersistenceService persistenceService;
    private final OperationService operationService;
    private final JsonService jsonService;

    @Inject
    public DagService(Config config, DagPersistenceService persistenceService, OperationService operationService, JsonService jsonService) {
        super(config, DagService.class);
        this.persistenceService = persistenceService;
        this.operationService = operationService;
        this.jsonService = jsonService;
    }

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
                        graph.addEdge(task.token(), depends);
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
                    taskBuilder.not(ImmutableSet.of(task.depends()).size() == task.depends().size()).accept("depends", "not unique");
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

    public MaybeErrors<Dag> createDag(Dag dag) {
        return validate(dag).maybe(() -> persistenceService.createDag(dag));
    }

    public MaybeErrors<Dag> updateDag(Dag dag) {
        return validate(dag).maybe(() -> persistenceService.updateDag(dag));
    }
}
