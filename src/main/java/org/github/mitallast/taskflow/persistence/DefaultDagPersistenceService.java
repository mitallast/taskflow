package org.github.mitallast.taskflow.persistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.common.json.JsonService;
import org.github.mitallast.taskflow.dag.*;
import org.github.mitallast.taskflow.operation.OperationCommand;
import org.github.mitallast.taskflow.operation.OperationEnvironment;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.github.mitallast.taskflow.operation.OperationStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
            Map<Long, ImmutableList.Builder<Task>> taskMap = new HashMap<>();
            context.selectFrom(table.task)
                .where(field.dag_id.in(ids))
                .fetch()
                .forEach(record -> taskMap.computeIfAbsent(record.get(field.dag_id), t -> new ImmutableList.Builder<>()).add(task(record)));

            ImmutableList.Builder<Dag> dags = ImmutableList.builder();
            dagList.forEach(dag -> dags.add(new Dag(
                dag.id(),
                dag.version(),
                dag.token(),
                taskMap.computeIfAbsent(dag.id(), t -> new ImmutableList.Builder<>()).build()
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
                        .fetch()
                        .forEach(t -> tasks.add(task(t)));
                    return dag(record, tasks.build());
                });
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
                        .fetch()
                        .forEach(t -> tasks.add(task(t)));
                    return dag(record, tasks.build());
                });
        }
    }

    // dag run api

    @Override
    public DagRun createDagRun(Dag dag) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                logger.info("insert dag run token={} version={}", dag.token(), dag.version());

                DateTime createdDate = DateTime.now();
                Timestamp created = new Timestamp(createdDate.getMillis());

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
                    );

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

    @Override
    public ImmutableList<DagRun> findDagRuns() {
        try (DSLContext context = persistence.context()) {
            Map<Long, ImmutableList.Builder<TaskRun>> taskRunMap = new HashMap<>();
            context.selectFrom(table.task_run)
                .orderBy(field.start_date.asc(), field.id.desc())
                .fetch()
                .forEach(record -> taskRunMap.computeIfAbsent(record.get(field.dag_run_id), t -> new ImmutableList.Builder<>()).add(taskRun(record)));

            ImmutableList.Builder<DagRun> dagRuns = ImmutableList.builder();
            context.selectFrom(table.dag_run)
                .orderBy(field.id.desc())
                .fetch()
                .map(record -> dagRuns.add(dagRun(record, taskRunMap.computeIfAbsent(record.get(field.id), t -> new ImmutableList.Builder<>()).build())));

            return dagRuns.build();
        }
    }

    @Override
    public ImmutableList<DagRun> findPendingDagRuns() {
        try (DSLContext context = persistence.context()) {
            List<DagRun> dagRunList = context.selectFrom(table.dag_run)
                .where(field.status.in(DagRunStatus.PENDING.name(), DagRunStatus.RUNNING.name()))
                .fetch()
                .map(record -> dagRun(record, ImmutableList.of()));

            List<Long> ids = dagRunList.stream().map(DagRun::id).collect(Collectors.toList());
            Map<Long, ImmutableList.Builder<TaskRun>> taskRunMap = new HashMap<>();
            context.selectFrom(table.task_run)
                .where(field.dag_run_id.in(ids))
                .orderBy(field.id.desc())
                .fetch()
                .forEach(record -> taskRunMap.computeIfAbsent(record.get(field.dag_run_id), t -> new ImmutableList.Builder<>()).add(taskRun(record)));

            ImmutableList.Builder<DagRun> dagRuns = ImmutableList.builder();
            dagRunList.forEach(dagRun -> dagRuns.add(new DagRun(
                dagRun.id(),
                dagRun.dagId(),
                dagRun.createdDate(),
                dagRun.startDate(),
                dagRun.finishDate(),
                dagRun.status(),
                taskRunMap.computeIfAbsent(dagRun.id(), t -> new ImmutableList.Builder<>()).build()
            )));

            return dagRuns.build();
        }
    }

    @Override
    public Optional<DagRun> findDagRun(long id) {
        try (DSLContext context = persistence.context()) {
            return context.selectFrom(table.dag_run)
                .where(field.id.eq(id))
                .fetchOptional()
                .map(record -> {
                    ImmutableList.Builder<TaskRun> tasks = new ImmutableList.Builder<>();
                    context.selectFrom(table.task_run)
                        .where(field.dag_run_id.eq(id))
                        .orderBy(field.start_date.asc(), field.id.desc())
                        .fetch()
                        .forEach(t -> tasks.add(taskRun(t)));
                    return dagRun(record, tasks.build());
                });
        }
    }

    @Override
    public boolean startDagRun(long id) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                int updated = DSL.using(conf)
                    .update(table.dag_run)
                    .set(field.status, DagRunStatus.RUNNING.name())
                    .set(field.start_date, now())
                    .where(field.id.eq(id).and(field.status.eq(DagRunStatus.PENDING.name())))
                    .execute();

                logger.info("updated {} rows", updated);
                return updated == 1;
            });
        }
    }

    @Override
    public boolean markDagRunSuccess(long id) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                int updated = DSL.using(conf)
                    .update(table.dag_run)
                    .set(field.status, DagRunStatus.SUCCESS.name())
                    .set(field.finish_date, now())
                    .where(field.id.eq(id).and(field.status.eq(DagRunStatus.RUNNING.name())))
                    .execute();

                logger.info("updated {} rows", updated);
                return updated == 1;
            });
        }
    }

    @Override
    public boolean markDagRunFailed(long id) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                int updated = DSL.using(conf)
                    .update(table.dag_run)
                    .set(field.status, DagRunStatus.FAILED.name())
                    .set(field.finish_date, now())
                    .where(field.id.eq(id).and(field.status.eq(DagRunStatus.RUNNING.name())))
                    .execute();

                logger.info("updated {} rows", updated);
                return updated == 1;
            });
        }
    }

    @Override
    public boolean markDagRunCanceled(long id) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                int updated = DSL.using(conf)
                    .update(table.dag_run)
                    .set(field.status, DagRunStatus.CANCELED.name())
                    .set(field.finish_date, now())
                    .where(field.id.eq(id).and(field.status.in(DagRunStatus.PENDING.name(), DagRunStatus.RUNNING.name())))
                    .execute();

                logger.info("updated {} rows", updated);
                return updated == 1;
            });
        }
    }

    // task run api

    @Override
    public TaskRun retry(TaskRun taskRun) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                logger.info("retry task run", taskRun.dagRunId(), taskRun.taskId());

                DateTime createdDate = DateTime.now();
                Timestamp created = new Timestamp(createdDate.getMillis());

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
                        val(taskRun.dagId()),
                        val(taskRun.taskId()),
                        val(taskRun.dagRunId()),
                        val(created),
                        val(TaskRunStatus.PENDING.name())
                    )
                    .returning(field.id)
                    .fetchOptional()
                    .orElseThrow(IllegalStateException::new)
                    .get(field.id);

                return new TaskRun(
                    taskRunId,
                    taskRun.dagId(),
                    taskRun.taskId(),
                    taskRun.dagRunId(),
                    createdDate,
                    null,
                    null,
                    TaskRunStatus.PENDING,
                    null
                );
            });
        }
    }

    @Override
    public boolean startTaskRun(long id) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                int updated = DSL.using(conf)
                    .update(table.task_run)
                    .set(field.status, TaskRunStatus.RUNNING.name())
                    .set(field.start_date, now())
                    .where(field.id.eq(id).and(field.status.eq(TaskRunStatus.PENDING.name())))
                    .execute();

                logger.info("updated {} rows", updated);
                return updated == 1;
            });
        }
    }

    @Override
    public boolean markTaskRunSuccess(long id, OperationResult operationResult) {
        Preconditions.checkNotNull(operationResult);
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                int updated = DSL.using(conf)
                    .update(table.task_run)
                    .set(field.status, TaskRunStatus.SUCCESS.name())
                    .set(field.finish_date, new Timestamp(System.currentTimeMillis()))
                    .set(field.operation_status, operationResult.status().name())
                    .set(field.operation_stdout, operationResult.stdout())
                    .set(field.operation_stderr, operationResult.stderr())
                    .where(field.id.eq(id).and(field.status.eq(TaskRunStatus.RUNNING.name())))
                    .execute();

                logger.info("updated {} rows", updated);
                return updated == 1;
            });
        }
    }

    @Override
    public boolean markTaskRunFailed(long id, OperationResult operationResult) {
        Preconditions.checkNotNull(operationResult);
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                int updated = DSL.using(conf)
                    .update(table.task_run)
                    .set(field.status, TaskRunStatus.FAILED.name())
                    .set(field.finish_date, now())
                    .set(field.operation_status, operationResult.status().name())
                    .set(field.operation_stdout, operationResult.stdout())
                    .set(field.operation_stderr, operationResult.stderr())
                    .where(field.id.eq(id).and(field.status.eq(TaskRunStatus.RUNNING.name())))
                    .execute();

                logger.info("updated {} rows", updated);
                return updated == 1;
            });
        }
    }

    @Override
    public boolean markTaskRunCanceled(long id) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                int updated = DSL.using(conf)
                    .update(table.task_run)
                    .set(field.status, TaskRunStatus.CANCELED.name())
                    .set(field.finish_date, now())
                    .where(field.id.eq(id).and(field.status.in(TaskRunStatus.PENDING.name(), TaskRunStatus.RUNNING.name())))
                    .execute();

                logger.info("updated {} rows", updated);
                return updated == 1;
            });
        }
    }

    // dag schedule api

    @Override
    public ImmutableList<DagSchedule> findDagSchedules() {
        try (DSLContext context = persistence.context()) {
            ImmutableList.Builder<DagSchedule> schedules = ImmutableList.builder();
            context.selectFrom(table.dag_schedule)
                .orderBy(field.enabled.desc(), field.token.asc())
                .fetch()
                .forEach(record -> schedules.add(dagSchedule(record)));

            return schedules.build();
        }
    }

    @Override
    public ImmutableList<DagSchedule> findEnabledDagSchedules() {
        try (DSLContext context = persistence.context()) {
            ImmutableList.Builder<DagSchedule> schedules = ImmutableList.builder();
            context.selectFrom(table.dag_schedule)
                .where(field.enabled.isTrue())
                .orderBy(field.token.asc())
                .fetch()
                .forEach(record -> schedules.add(dagSchedule(record)));

            return schedules.build();
        }
    }

    @Override
    public Optional<DagSchedule> findDagSchedule(String token) {
        try (DSLContext context = persistence.context()) {
            return context.selectFrom(table.dag_schedule)
                .where(field.token.eq(token))
                .fetchOptional()
                .map(this::dagSchedule);
        }
    }

    @Override
    public boolean markDagScheduleEnabled(String token) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                int updated = DSL.using(conf)
                    .update(table.dag_schedule)
                    .set(field.enabled, true)
                    .where(field.token.eq(token))
                    .execute();

                logger.info("updated {} rows", updated);
                return updated == 1;
            });
        }
    }

    @Override
    public boolean markDagScheduleDisabled(String token) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                int updated = DSL.using(conf)
                    .update(table.dag_schedule)
                    .set(field.enabled, false)
                    .where(field.token.eq(token))
                    .execute();

                logger.info("updated {} rows", updated);
                return updated == 1;
            });
        }
    }

    @Override
    public boolean updateSchedule(DagSchedule dagSchedule) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                int updated = DSL.using(conf)
                    .update(table.dag_schedule)
                    .set(field.enabled, dagSchedule.isEnabled())
                    .set(field.cron_expression, dagSchedule.cronExpression())
                    .where(field.token.eq(dagSchedule.token()))
                    .execute();

                logger.info("updated {} rows", updated);
                return updated == 1;
            });
        }
    }

    // private api

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

    private TaskRun taskRun(Record record) {
        return new TaskRun(
            record.get(field.id),
            record.get(field.dag_id),
            record.get(field.task_id),
            record.get(field.dag_run_id),
            date(record.get(field.created_date)),
            date(record.get(field.start_date)),
            date(record.get(field.finish_date)),
            TaskRunStatus.valueOf(record.get(field.status)),
            operationResult(record)
        );
    }

    private DagRun dagRun(Record record, ImmutableList<TaskRun> tasks) {
        return new DagRun(
            record.get(field.id),
            record.get(field.dag_id),
            date(record.get(field.created_date)),
            date(record.get(field.start_date)),
            date(record.get(field.finish_date)),
            DagRunStatus.valueOf(record.get(field.status)),
            tasks
        );
    }

    private DagSchedule dagSchedule(Record record) {
        return new DagSchedule(
            record.get(field.token),
            record.get(field.enabled),
            record.get(field.cron_expression)
        );
    }

    private OperationResult operationResult(Record record) {
        String status = record.get(field.operation_status);
        if (status == null) {
            return null;
        } else {
            return new OperationResult(
                OperationStatus.valueOf(status),
                record.get(field.operation_stdout),
                record.get(field.operation_stderr)
            );
        }
    }

    private Timestamp now() {
        return new Timestamp(System.currentTimeMillis());
    }

    private DateTime date(Timestamp timestamp) {
        if (timestamp == null) {
            return null;
        } else {
            return new DateTime(timestamp.getTime(), DateTimeZone.UTC);
        }
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
