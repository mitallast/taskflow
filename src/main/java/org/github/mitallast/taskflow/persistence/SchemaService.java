package org.github.mitallast.taskflow.persistence;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.jooq.DSLContext;

import static org.jooq.impl.DSL.constraint;

public class SchemaService extends AbstractComponent {

    private final PersistenceService persistence;

    @Inject
    public SchemaService(Config config, PersistenceService persistence) {
        super(config.getConfig("persistence"), SchemaService.class);
        this.persistence = persistence;

        process();
    }

    public void process() {
        try (DSLContext context = persistence.context()) {

            if (this.config.getBoolean("cleanup")) {
                context.dropTableIfExists(Schema.table.task_run).execute();
                context.dropTableIfExists(Schema.table.dag_run).execute();
                context.dropTableIfExists(Schema.table.task).execute();
                context.dropTableIfExists(Schema.table.dag).execute();
                context.dropTableIfExists(Schema.table.dag_schedule).execute();

                context.dropSequenceIfExists(Schema.sequence.dag_seq).execute();
                context.dropSequenceIfExists(Schema.sequence.task_seq).execute();
                context.dropSequenceIfExists(Schema.sequence.dag_run_seq).execute();
                context.dropSequenceIfExists(Schema.sequence.task_run_seq).execute();
            }

            context.createSequenceIfNotExists(Schema.sequence.dag_seq).execute();
            context.createSequenceIfNotExists(Schema.sequence.task_seq).execute();
            context.createSequenceIfNotExists(Schema.sequence.dag_run_seq).execute();
            context.createSequenceIfNotExists(Schema.sequence.task_run_seq).execute();

            context.createTableIfNotExists(Schema.table.dag)
                .column(Schema.field.id)
                .column(Schema.field.version)
                .column(Schema.field.token)
                .column(Schema.field.latest)
                .constraint(constraint().primaryKey(Schema.field.id))
                .constraint(constraint("dag_version").unique(Schema.field.token, Schema.field.version))
                .execute();

            context.createUniqueIndexIfNotExists("dag_token_version_latest")
                .on(Schema.table.dag, Schema.field.token, Schema.field.latest)
                .where(Schema.field.latest.isTrue())
                .execute();

            context.createTableIfNotExists(Schema.table.task)
                .column(Schema.field.id)
                .column(Schema.field.version)
                .column(Schema.field.token)
                .column(Schema.field.dag_id)
                .column(Schema.field.depends)
                .column(Schema.field.retry)
                .column(Schema.field.operation)
                .column(Schema.field.operation_config)
                .column(Schema.field.operation_environment)
                .constraint(constraint().primaryKey(Schema.field.id))
                .constraint(constraint("dag_task_version").unique(Schema.field.dag_id, Schema.field.token, Schema.field.version))
                .constraint(constraint("task_fk_dag").foreignKey(Schema.field.dag_id).references(Schema.table.dag, Schema.field.id))
                .execute();

            context.createTableIfNotExists(Schema.table.dag_run)
                .column(Schema.field.id)
                .column(Schema.field.dag_id)
                .column(Schema.field.created_date)
                .column(Schema.field.start_date)
                .column(Schema.field.finish_date)
                .column(Schema.field.status)
                .constraint(constraint().primaryKey(Schema.field.id))
                .constraint(constraint("dag_run_fk_dag").foreignKey(Schema.field.dag_id).references(Schema.table.dag, Schema.field.id))
                .execute();

            context.createTableIfNotExists(Schema.table.task_run)
                .column(Schema.field.id)
                .column(Schema.field.dag_id)
                .column(Schema.field.task_id)
                .column(Schema.field.dag_run_id)
                .column(Schema.field.created_date)
                .column(Schema.field.start_date)
                .column(Schema.field.finish_date)
                .column(Schema.field.status)
                .column(Schema.field.operation_status)
                .column(Schema.field.operation_stdout)
                .column(Schema.field.operation_stderr)
                .constraint(constraint().primaryKey(Schema.field.id))
                .constraint(constraint("task_run_fk_dag_run").foreignKey(Schema.field.dag_run_id).references(Schema.table.dag_run, Schema.field.id))
                .constraint(constraint("task_run_fk_dag").foreignKey(Schema.field.dag_id).references(Schema.table.dag, Schema.field.id))
                .constraint(constraint("task_run_fk_task").foreignKey(Schema.field.task_id).references(Schema.table.task, Schema.field.id))
                .execute();

            context.createTableIfNotExists(Schema.table.dag_schedule)
                .column(Schema.field.token)
                .column(Schema.field.enabled)
                .column(Schema.field.cron_expression)
                .constraint(constraint().primaryKey(Schema.field.token))
                .execute();
        }
    }
}
