package org.github.mitallast.taskflow.dag;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.operation.OperationCommand;
import org.github.mitallast.taskflow.operation.OperationEnvironment;
import org.github.mitallast.taskflow.persistence.PersistenceService;
import org.joda.time.DateTime;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.*;

public class DagPersistenceService extends AbstractComponent {

    private final PersistenceService persistence;

    private static class sequence {
        private final static Sequence<Long> dag_seq = sequence("dag_seq", SQLDataType.BIGINT);
        private final static Sequence<Long> task_seq = sequence("task_seq", SQLDataType.BIGINT);
        private final static Sequence<Long> dag_run_seq = sequence("dag_run_seq", SQLDataType.BIGINT);
        private final static Sequence<Long> task_run_seq = sequence("task_run_seq", SQLDataType.BIGINT);
    }

    private static class table {
        private final static Table<Record> dag = table("dag");
        private final static Table<Record> task = table("task");
        private final static Table<Record> dag_run = table("dag_run");
        private final static Table<Record> task_run = table("task_run");
    }

    private static class field {
        private final static Field<Long> id = field("id", SQLDataType.BIGINT.nullable(false));
        private final static Field<Long> dag_id = field("dag_id", SQLDataType.BIGINT.nullable(false));
        private final static Field<Long> dag_run_id = field("dag_run_id", SQLDataType.BIGINT.nullable(false));
        private final static Field<Long> task_id = field("task_id", SQLDataType.BIGINT.nullable(false));

        private final static Field<Boolean> latest = field("latest", SQLDataType.BOOLEAN.nullable(false));

        private final static Field<Integer> version = field("version", SQLDataType.INTEGER.nullable(false));

        private final static Field<Timestamp> created_date = field("created_date", SQLDataType.TIMESTAMP.nullable(false));
        private final static Field<Timestamp> start_date = field("start_date", SQLDataType.TIMESTAMP.nullable(true));
        private final static Field<Timestamp> end_date = field("end_date", SQLDataType.TIMESTAMP.nullable(true));

        private final static Field<String> token = field("token", SQLDataType.VARCHAR(256).nullable(false));
        private final static Field<String> operation = field("operation", SQLDataType.VARCHAR(256).nullable(false));

        private final static Field<String> status = field("status", SQLDataType.VARCHAR(32).nullable(false));
        private final static Field<String> operation_status = field("operation_status", SQLDataType.VARCHAR(32));

        private final static Field<byte[]> depends = field("depends", SQLDataType.BLOB.nullable(false));
        private final static Field<byte[]> operation_config = field("operation_config", SQLDataType.BLOB.nullable(false));
        private final static Field<byte[]> operation_environment = field("operation_environment", SQLDataType.BLOB.nullable(false));
        private final static Field<byte[]> operation_stdout = field("operation_stdout", SQLDataType.BLOB);
        private final static Field<byte[]> operation_stderr = field("operation_stderr", SQLDataType.BLOB);
    }

    @Inject
    public DagPersistenceService(Config config, PersistenceService persistence) {
        super(config, DagPersistenceService.class);
        this.persistence = persistence;

        try (DSLContext context = persistence.context()) {

            context.dropTableIfExists(table.task_run).execute();
            context.dropTableIfExists(table.dag_run).execute();
            context.dropTableIfExists(table.task).execute();
            context.dropTableIfExists(table.dag).execute();

            context.dropSequenceIfExists(sequence.dag_seq).execute();
            context.dropSequenceIfExists(sequence.task_seq).execute();
            context.dropSequenceIfExists(sequence.dag_run_seq).execute();
            context.dropSequenceIfExists(sequence.task_run_seq).execute();

            context.createSequenceIfNotExists(sequence.dag_seq).execute();
            context.createSequenceIfNotExists(sequence.task_seq).execute();
            context.createSequenceIfNotExists(sequence.dag_run_seq).execute();
            context.createSequenceIfNotExists(sequence.task_run_seq).execute();

            context.createTableIfNotExists(table.dag)
                .column(field.id)
                .column(field.version)
                .column(field.token)
                .column(field.latest)
                .constraint(constraint().primaryKey(field.id))
                .constraint(constraint("dag_version").unique(field.token, field.version))
                .execute();

            context.createTableIfNotExists(table.task)
                .column(field.id)
                .column(field.version)
                .column(field.token)
                .column(field.dag_id)
                .column(field.depends)
                .column(field.operation)
                .column(field.operation_config)
                .column(field.operation_environment)
                .constraint(constraint().primaryKey(field.id))
                .constraint(constraint("dag_task_version").unique(field.dag_id, field.token, field.version))
                .constraint(constraint("task_fk_dag").foreignKey(field.dag_id).references(table.dag, field.id))
                .execute();

            context.createTableIfNotExists(table.dag_run)
                .column(field.id)
                .column(field.dag_id)
                .column(field.created_date)
                .column(field.start_date)
                .column(field.end_date)
                .column(field.status)
                .constraint(constraint().primaryKey(field.id))
                .constraint(constraint("dag_run_fk_dag").foreignKey(field.dag_id).references(table.dag, field.id))
                .execute();

            context.createTableIfNotExists(table.task_run)
                .column(field.id)
                .column(field.dag_id)
                .column(field.task_id)
                .column(field.dag_run_id)
                .column(field.created_date)
                .column(field.start_date)
                .column(field.end_date)
                .column(field.status)
                .column(field.operation_status)
                .column(field.operation_stdout)
                .column(field.operation_stderr)
                .constraint(constraint().primaryKey(field.id))
                .constraint(constraint("task_run_fk_dag_run").foreignKey(field.dag_run_id).references(table.dag_run, field.id))
                .constraint(constraint("task_run_fk_dag").foreignKey(field.dag_id).references(table.dag, field.id))
                .constraint(constraint("task_run_fk_task").foreignKey(field.task_id).references(table.task_run, field.id))
                .execute();
        }
    }

    /**
     * Create dag with version 0
     */
    public Dag createDag(Dag dag) {
        return insertDag(dag, 0);
    }

    /**
     * Create dag with next version
     */
    public Dag updateDag(Dag dag) {
        return insertDag(dag, dag.version() + 1);
    }

    private Dag insertDag(Dag dag, int version) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                logger.info("insert dag {} {}", dag.token(), version);

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
                            field.operation,
                            field.operation_config,
                            field.operation_environment)
                        .values(
                            sequence.task_seq.nextval(),
                            val(version),
                            val(task.token()),
                            val(dagId),
                            val(serialize(task.depends())),
                            val(task.operation()),
                            val(serialize(task.command().config())),
                            val(serialize(task.command().environment().map()))
                        )
                        .returning(field.id)
                        .fetchOptional()
                        .orElseThrow(IllegalStateException::new)
                        .get(field.id);

                    logger.info("task id={} token={}", taskId, task.token());

                    tasks.add(new Task(taskId, version, task.token(), task.depends(), task.operation(), task.command()));
                }

                return new Dag(dagId, version, dag.token(), tasks.build());
            });
        }
    }

    /**
     * Find dag with latest version
     */
    public ImmutableList<Dag> findLatestDags() {
        try (DSLContext context = persistence.context()) {
            List<Dag> dagList = context.selectFrom(table.dag)
                .where(field.latest.isTrue())
                .orderBy(field.id.asc())
                .fetch()
                .map(record -> dag(record, ImmutableList.of()));

            List<Long> ids = dagList.stream().map(Dag::id).collect(Collectors.toList());

            Map<Long, ImmutableList.Builder<Task>> taskMap = new HashMap<>();
            context.selectFrom(table.task)
                .where(field.dag_id.in(ids))
                .fetch()
                .forEach(record -> taskMap.computeIfAbsent(record.get(field.dag_id), t -> new ImmutableList.Builder<>()).add(task(record)));

            return ImmutableList.copyOf(dagList);
        }
    }

    /**
     * Find dag by id
     */
    public Optional<Dag> findDag(long id) {
        try (DSLContext context = persistence.context()) {
            return context.selectFrom(table.dag)
                .where(field.id.eq(id))
                .fetchOptional()
                .map(record -> {
                    ImmutableList.Builder<Task> tasks = new ImmutableList.Builder<>();
                    context.selectFrom(table.task)
                        .where(field.dag_id.eq(id))
                        .fetch()
                        .forEach(t -> tasks.add(task(t)));
                    return dag(record, tasks.build());
                });
        }
    }

    /**
     * Find dag by token
     */
    public Optional<Dag> findDag(String token) {
        try (DSLContext context = persistence.context()) {
            return context.selectFrom(table.dag)
                .where(field.token.eq(token).and(field.latest.isTrue()))
                .fetchOptional()
                .map(record -> {
                    ImmutableList.Builder<Task> tasks = new ImmutableList.Builder<>();
                    context.selectFrom(table.task)
                        .where(field.dag_id.eq(record.get(field.id)))
                        .fetch()
                        .forEach(t -> tasks.add(task(t)));
                    return dag(record, tasks.build());
                });
        }
    }

    /**
     * Schedule dag run
     */
    public DagRun createDagRun(Dag dag) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                logger.info("insert dag run token={} version={}", dag.token(), dag.version());

                DateTime createdDate = DateTime.now();
                Timestamp created = new Timestamp(createdDate.getMillis());


                long dagRunId = DSL.using(conf)
                    .insertInto(
                        table.dag_run,
                        field.id,
                        field.dag_id,
                        field.created_date,
                        field.status
                    )
                    .values(
                        sequence.dag_run_seq.nextval(),
                        val(dag.id()),
                        val(created),
                        val(DagRunStatus.PENDING.name())
                    )
                    .returning(field.id)
                    .fetchOptional()
                    .orElseThrow(IllegalStateException::new)
                    .get(field.id);

                logger.info("dag run id={} token={} version={}", dagRunId, dag.token(), dag.version());

                ImmutableList.Builder<TaskRun> tasks = ImmutableList.builder();

                for (Task task : dag.tasks()) {
                    long taskRunId = DSL.using(conf)
                        .insertInto(
                            table.task_run,
                            field.id,
                            field.dag_id,
                            field.task_id,
                            field.dag_run_id,
                            field.created_date,
                            field.status
                        )
                        .values(
                            sequence.task_run_seq.nextval(),
                            val(dag.id()),
                            val(task.id()),
                            val(dagRunId),
                            val(created),
                            val(TaskRunStatus.PENDING.name())
                        )
                        .returning(field.id)
                        .fetchOptional()
                        .orElseThrow(IllegalStateException::new)
                        .get(field.id);

                    tasks.add(new TaskRun(
                        taskRunId,
                        dag.id(),
                        task.id(),
                        dagRunId,
                        createdDate,
                        null,
                        null,
                        TaskRunStatus.PENDING,
                        null
                    ));
                }

                return new DagRun(
                    dagRunId,
                    dag.id(),
                    createdDate,
                    null,
                    null,
                    DagRunStatus.PENDING,
                    tasks.build()
                );
            });
        }
    }

    private static Dag dag(Record record, ImmutableList<Task> tasks) {
        return new Dag(
            record.get(field.id),
            record.get(field.version),
            record.get(field.token),
            tasks
        );
    }

    private static Task task(Record record) {
        return new Task(
            record.get(field.id),
            record.get(field.version),
            record.get(field.token),
            deserializeTokens(record.get(field.depends)),
            record.get(field.operation),
            new OperationCommand(
                deserializeConfig(record.get(field.operation_config)),
                new OperationEnvironment(deserializeMap(record.get(field.operation_environment)))
            )
        );
    }

    private static byte[] serialize(ImmutableList<String> tokens) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        new ObjectMapper().writeValue(output, tokens);
        return output.toByteArray();
    }

    private static ImmutableList<String> deserializeTokens(byte[] data) {
        ByteArrayInputStream input = new ByteArrayInputStream(data);
        try {
            List<String> list = new ObjectMapper().readValue(input, new TypeReference<List<String>>() {
            });

            return ImmutableList.copyOf(list);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private static byte[] serialize(Config config) {
        return config.root()
            .render(ConfigRenderOptions.concise())
            .getBytes(Charset.forName("UTF-8"));
    }

    private static Config deserializeConfig(byte[] data) {
        String str = new String(data, Charset.forName("UTF-8"));
        return ConfigFactory.parseString(str);
    }

    private static byte[] serialize(ImmutableMap<String, String> map) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        new ObjectMapper().writeValue(output, map);
        return output.toByteArray();
    }

    private static ImmutableMap<String, String> deserializeMap(byte[] data) {
        ByteArrayInputStream input = new ByteArrayInputStream(data);
        try {
            Map<String, String> map = new ObjectMapper().readValue(input, new TypeReference<Map<String, String>>() {
            });
            return ImmutableMap.copyOf(map);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
