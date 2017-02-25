package org.github.mitallast.taskflow.persistence;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.dag.*;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.github.mitallast.taskflow.operation.OperationStatus;
import org.github.mitallast.taskflow.persistence.Schema.field;
import org.github.mitallast.taskflow.persistence.Schema.sequence;
import org.github.mitallast.taskflow.persistence.Schema.table;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.github.mitallast.taskflow.common.Immutable.*;
import static org.jooq.impl.DSL.val;

public class DefaultDagRunPersistenceService extends AbstractComponent implements DagRunPersistenceService {

    private final PersistenceService persistence;
    private final DagPersistenceService dagPersistence;

    @Inject
    public DefaultDagRunPersistenceService(Config config, PersistenceService persistence, DagPersistenceService dagPersistence) {
        super(config.getConfig("persistence"), DagPersistenceService.class);
        this.persistence = persistence;
        this.dagPersistence = dagPersistence;
    }

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
                            field.task_id,
                            field.dag_run_id,
                            field.created_date,
                            field.status
                        )
                        .values(
                            sequence.task_run_seq.nextval(),
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
                        task,
                        createdDate,
                        null,
                        null,
                        TaskRunStatus.PENDING,
                        null
                    ));
                }

                return new DagRun(
                    dagRunId,
                    dag,
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
        return findByCondition();
    }

    @Override
    public ImmutableList<DagRun> findPendingDagRuns() {
        return findByCondition(
            field.status.in(DagRunStatus.PENDING.name(), DagRunStatus.RUNNING.name())
        );
    }

    @Override
    public ImmutableList<DagRun> findPendingDagRunsByDag(long dagId) {
        return findByCondition(
            field.status.in(DagRunStatus.PENDING.name(), DagRunStatus.RUNNING.name()),
            field.dag_id.eq(dagId)
        );
    }

    @Override
    public Optional<DagRun> findDagRun(long id) {
        return headOpt(findByCondition(field.id.eq(id)));
    }

    private ImmutableList<DagRun> findByCondition(Condition... conditions) {
        try (DSLContext context = persistence.context()) {
            List<DagRun> dagRunList = context.selectFrom(table.dag_run)
                .where(conditions)
                .orderBy(field.id.desc())
                .fetch()
                .map(record -> dagRun(record, new Dag(record.get(field.dag_id)), ImmutableList.of()));

            List<Dag> dags = dagPersistence.findDagByIds(map(map(dagRunList, DagRun::dag), Dag::id));
            Map<Long, Dag> dagMap = group(dags, Dag::id);
            Map<Long, Task> taskMap = group(flatMap(dags, Dag::tasks), Task::id);

            ImmutableListMultimap.Builder<Long, TaskRun> taskRunBuilder = ImmutableListMultimap.builder();
            context.selectFrom(table.task_run)
                .where(field.dag_run_id.in(map(dagRunList, DagRun::id)))
                .orderBy(field.start_date.asc().nullsLast(), field.id.desc())
                .fetch()
                .forEach(record -> taskRunBuilder.put(record.get(field.dag_run_id), taskRun(record, taskMap.get(record.get(field.task_id)))));
            ImmutableListMultimap<Long, TaskRun> taskRunMap = taskRunBuilder.build();

            ImmutableList.Builder<DagRun> dagRuns = ImmutableList.builder();
            dagRunList.forEach(dagRun -> dagRuns.add(new DagRun(
                dagRun.id(),
                dagMap.get(dagRun.dag().id()),
                dagRun.createdDate(),
                dagRun.startDate(),
                dagRun.finishDate(),
                dagRun.status(),
                taskRunMap.get(dagRun.id())
            )));

            return dagRuns.build();
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
    public TaskRun retry(DagRun dagRun, TaskRun taskRun) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                logger.info("retry task run {} task {}", taskRun.id(), taskRun.task().token());

                DateTime createdDate = DateTime.now();
                Timestamp created = new Timestamp(createdDate.getMillis());

                long taskRunId = DSL.using(conf)
                    .insertInto(
                        table.task_run,
                        field.id,
                        field.task_id,
                        field.dag_run_id,
                        field.created_date,
                        field.status
                    )
                    .values(
                        sequence.task_run_seq.nextval(),
                        val(taskRun.task().id()),
                        val(dagRun.id()),
                        val(created),
                        val(TaskRunStatus.PENDING.name())
                    )
                    .returning(field.id)
                    .fetchOptional()
                    .orElseThrow(IllegalStateException::new)
                    .get(field.id);

                return new TaskRun(
                    taskRunId,
                    taskRun.task(),
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
                    .set(field.operation_output, operationResult.output())
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
                    .set(field.operation_output, operationResult.output())
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

    private TaskRun taskRun(Record record, Task task) {
        return new TaskRun(
            record.get(field.id),
            task,
            date(record.get(field.created_date)),
            date(record.get(field.start_date)),
            date(record.get(field.finish_date)),
            TaskRunStatus.valueOf(record.get(field.status)),
            operationResult(record)
        );
    }

    private DagRun dagRun(Record record, Dag dag, ImmutableList<TaskRun> tasks) {
        return new DagRun(
            record.get(field.id),
            dag,
            date(record.get(field.created_date)),
            date(record.get(field.start_date)),
            date(record.get(field.finish_date)),
            DagRunStatus.valueOf(record.get(field.status)),
            tasks
        );
    }

    private OperationResult operationResult(Record record) {
        String status = record.get(field.operation_status);
        if (status == null) {
            return null;
        } else {
            return new OperationResult(
                OperationStatus.valueOf(status),
                record.get(field.operation_output)
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
}
