package org.github.mitallast.taskflow.persistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.common.json.JsonService;
import org.github.mitallast.taskflow.dag.Dag;
import org.github.mitallast.taskflow.dag.DagPersistenceService;
import org.github.mitallast.taskflow.dag.Task;
import org.github.mitallast.taskflow.operation.OperationCommand;
import org.github.mitallast.taskflow.operation.OperationEnvironment;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.github.mitallast.taskflow.persistence.Schema.*;
import static org.jooq.impl.DSL.val;

public class DefaultDagPersistenceService extends AbstractComponent implements DagPersistenceService {

    private final PersistenceService persistence;
    private final JsonService jsonService;

    @Inject
    public DefaultDagPersistenceService(Config config, PersistenceService persistence, JsonService jsonService) {
        super(config.getConfig("persistence"), DagPersistenceService.class);
        this.persistence = persistence;
        this.jsonService = jsonService;
    }

    /**
     * Create dag with version 0
     */
    @Override
    public Dag createDag(Dag dag) {
        return insertDag(dag, 0);
    }

    /**
     * Create dag with next version
     */
    @Override
    public Dag updateDag(Dag dag) {
        return insertDag(dag, dag.version() + 1);
    }

    private Dag insertDag(Dag dag, int version) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                logger.info("insert dag {} {}", dag.token(), version);

                DSL.using(conf)
                    .insertInto(
                        table.dag_schedule,
                        field.token,
                        field.enabled,
                        field.cron_expression
                    )
                    .values(
                        dag.token(),
                        false,
                        null
                    )
                    .onConflictDoNothing()
                    .execute();

                int updated = DSL.using(conf)
                    .update(table.dag)
                    .set(field.latest, false)
                    .where(field.token.eq(dag.token()))
                    .execute();
                logger.info("updated {} rows", updated);

                long dagId = DSL.using(conf)
                    .insertInto(
                        table.dag,
                        field.id,
                        field.version,
                        field.latest,
                        field.token
                    )
                    .values(
                        sequence.dag_seq.nextval(),
                        val(version),
                        val(true),
                        val(dag.token())
                    )
                    .returning(field.id)
                    .fetchOptional()
                    .orElseThrow(IllegalStateException::new)
                    .get(field.id);

                logger.info("dag id={} token={}", dagId, dag.token());

                ImmutableList.Builder<Task> tasks = ImmutableList.builder();

                for (Task task : dag.tasks()) {
                    long taskId = DSL.using(conf)
                        .insertInto(
                            table.task,
                            field.id,
                            field.version,
                            field.token,
                            field.dag_id,
                            field.depends,
                            field.retry,
                            field.operation,
                            field.operation_config,
                            field.operation_environment)
                        .values(
                            sequence.task_seq.nextval(),
                            val(version),
                            val(task.token()),
                            val(dagId),
                            val(serialize(task.depends())),
                            val(task.retry()),
                            val(task.operation()),
                            val(serialize(task.command().config())),
                            val(serialize(task.command().environment().map()))
                        )
                        .returning(field.id)
                        .fetchOptional()
                        .orElseThrow(IllegalStateException::new)
                        .get(field.id);

                    logger.info("task id={} token={}", taskId, task.token());

                    tasks.add(new Task(taskId, version, task.token(), task.depends(), task.retry(), task.operation(), task.command()));
                }

                return new Dag(dagId, version, dag.token(), tasks.build());
            });
        }
    }

    @Override
    public ImmutableList<Dag> findLatestDags() {
        try (DSLContext context = persistence.context()) {
            List<Dag> dagList = context.selectFrom(table.dag)
                .where(field.latest.isTrue())
                .orderBy(field.id.asc())
                .fetch()
                .map(record -> dag(record, ImmutableList.of()));

            List<Long> ids = dagList.stream().map(Dag::id).collect(Collectors.toList());
            ImmutableListMultimap.Builder<Long, Task> tasksBuilder = ImmutableListMultimap.builder();
            context.selectFrom(table.task)
                .where(field.dag_id.in(ids))
                .orderBy(field.id.asc())
                .fetch()
                .forEach(record -> tasksBuilder.put(record.get(field.dag_id), task(record)));
            ImmutableListMultimap<Long, Task> tasks = tasksBuilder.build();

            ImmutableList.Builder<Dag> dags = ImmutableList.builder();
            dagList.forEach(dag -> dags.add(new Dag(
                dag.id(),
                dag.version(),
                dag.token(),
                tasks.get(dag.id())
            )));

            return dags.build();
        }
    }

    @Override
    public Optional<Dag> findDagById(long id) {
        try (DSLContext context = persistence.context()) {
            return context.selectFrom(table.dag)
                .where(field.id.eq(id))
                .fetchOptional()
                .map(record -> {
                    ImmutableList.Builder<Task> tasks = new ImmutableList.Builder<>();
                    context.selectFrom(table.task)
                        .where(field.dag_id.eq(id))
                        .orderBy(field.id.asc())
                        .fetch()
                        .forEach(t -> tasks.add(task(t)));
                    return dag(record, tasks.build());
                });
        }
    }

    @Override
    public ImmutableList<Dag> findDagByIds(Collection<Long> ids) {
        try (DSLContext context = persistence.context()) {
            ImmutableListMultimap.Builder<Long, Task> tasksBuilder = ImmutableListMultimap.builder();
            context.selectFrom(table.task)
                .where(field.dag_id.in(ids))
                .orderBy(field.id.asc())
                .fetch()
                .forEach(record -> tasksBuilder.put(record.get(field.dag_id), task(record)));
            ImmutableListMultimap<Long, Task> tasks = tasksBuilder.build();

            ImmutableList.Builder<Dag> dags = ImmutableList.builder();
            context.selectFrom(table.dag)
                .orderBy(field.id.asc())
                .fetch()
                .forEach(record -> dags.add(dag(record, tasks)));

            return dags.build();
        }
    }

    @Override
    public Optional<Dag> findDagByToken(String token) {
        try (DSLContext context = persistence.context()) {
            return context.selectFrom(table.dag)
                .where(field.token.eq(token).and(field.latest.isTrue()))
                .fetchOptional()
                .map(record -> {
                    ImmutableList.Builder<Task> tasks = new ImmutableList.Builder<>();
                    context.selectFrom(table.task)
                        .where(field.dag_id.eq(record.get(field.id)))
                        .orderBy(field.id.asc())
                        .fetch()
                        .forEach(t -> tasks.add(task(t)));
                    return dag(record, tasks.build());
                });
        }
    }

    private Dag dag(Record record, ImmutableListMultimap<Long, Task> tasks) {
        return dag(record, tasks.get(record.get(field.id)));
    }

    private Dag dag(Record record, ImmutableList<Task> tasks) {
        return new Dag(
            record.get(field.id),
            record.get(field.version),
            record.get(field.token),
            tasks
        );
    }

    private Task task(Record record) {
        return new Task(
            record.get(field.id),
            record.get(field.version),
            record.get(field.token),
            deserializeTokens(record.get(field.depends)),
            record.get(field.retry),
            record.get(field.operation),
            new OperationCommand(
                deserializeConfig(record.get(field.operation_config)),
                new OperationEnvironment(deserializeMap(record.get(field.operation_environment)))
            )
        );
    }

    private String serialize(ImmutableSet<String> tokens) throws IOException {
        return jsonService.serialize(tokens);
    }

    private ImmutableSet<String> deserializeTokens(String data) {
        TypeReference<ImmutableSet<String>> type = new TypeReference<ImmutableSet<String>>() {
        };
        return jsonService.deserialize(data, type);
    }

    private String serialize(Config config) {
        return config.root().render(ConfigRenderOptions.concise());
    }

    private Config deserializeConfig(String data) {
        return ConfigFactory.parseString(data);
    }

    private String serialize(ImmutableMap<String, String> map) throws IOException {
        return jsonService.serialize(map);
    }

    private ImmutableMap<String, String> deserializeMap(String data) {
        TypeReference<ImmutableMap<String, String>> type = new TypeReference<ImmutableMap<String, String>>() {
        };
        return jsonService.deserialize(data, type);
    }
}
