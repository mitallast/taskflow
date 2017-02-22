package org.github.mitallast.taskflow.persistence;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.dag.DagSchedule;
import org.github.mitallast.taskflow.dag.DagSchedulePersistenceService;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;

import java.util.Optional;

public class DefaultDagSchedulePersistenceService extends AbstractComponent implements DagSchedulePersistenceService {

    private final PersistenceService persistence;

    @Inject
    public DefaultDagSchedulePersistenceService(Config config, PersistenceService persistence) {
        super(config.getConfig("persistence"), DefaultDagSchedulePersistenceService.class);
        this.persistence = persistence;
    }

    @Override
    public ImmutableList<DagSchedule> findDagSchedules() {
        try (DSLContext context = persistence.context()) {
            ImmutableList.Builder<DagSchedule> schedules = ImmutableList.builder();
            context.selectFrom(Schema.table.dag_schedule)
                .orderBy(Schema.field.enabled.desc(), Schema.field.token.asc())
                .fetch()
                .forEach(record -> schedules.add(dagSchedule(record)));

            return schedules.build();
        }
    }

    @Override
    public ImmutableList<DagSchedule> findEnabledDagSchedules() {
        try (DSLContext context = persistence.context()) {
            ImmutableList.Builder<DagSchedule> schedules = ImmutableList.builder();
            context.selectFrom(Schema.table.dag_schedule)
                .where(Schema.field.enabled.isTrue())
                .orderBy(Schema.field.token.asc())
                .fetch()
                .forEach(record -> schedules.add(dagSchedule(record)));

            return schedules.build();
        }
    }

    @Override
    public Optional<DagSchedule> findDagSchedule(String token) {
        try (DSLContext context = persistence.context()) {
            return context.selectFrom(Schema.table.dag_schedule)
                .where(Schema.field.token.eq(token))
                .fetchOptional()
                .map(this::dagSchedule);
        }
    }

    @Override
    public boolean markDagScheduleEnabled(String token) {
        try (DSLContext tr = persistence.context()) {
            return tr.transactionResult(conf -> {
                int updated = DSL.using(conf)
                    .update(Schema.table.dag_schedule)
                    .set(Schema.field.enabled, true)
                    .where(Schema.field.token.eq(token))
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
                    .update(Schema.table.dag_schedule)
                    .set(Schema.field.enabled, false)
                    .where(Schema.field.token.eq(token))
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
                    .update(Schema.table.dag_schedule)
                    .set(Schema.field.enabled, dagSchedule.isEnabled())
                    .set(Schema.field.cron_expression, dagSchedule.cronExpression())
                    .where(Schema.field.token.eq(dagSchedule.token()))
                    .execute();

                logger.info("updated {} rows", updated);
                return updated == 1;
            });
        }
    }

    private DagSchedule dagSchedule(Record record) {
        return new DagSchedule(
            record.get(Schema.field.token),
            record.get(Schema.field.enabled),
            record.get(Schema.field.cron_expression)
        );
    }
}
