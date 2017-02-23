package org.github.mitallast.taskflow.persistence;

import org.github.mitallast.taskflow.dag.DagRunStatus;
import org.github.mitallast.taskflow.dag.TaskRunStatus;
import org.github.mitallast.taskflow.operation.OperationStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.jooq.*;
import org.jooq.impl.AbstractConverter;
import org.jooq.impl.SQLDataType;

import java.sql.Timestamp;

import static org.jooq.impl.DSL.*;

interface Schema {
    interface sequence {
        Sequence<Long> dag_seq = sequence("dag_seq", SQLDataType.BIGINT);
        Sequence<Long> task_seq = sequence("task_seq", SQLDataType.BIGINT);
        Sequence<Long> dag_run_seq = sequence("dag_run_seq", SQLDataType.BIGINT);
        Sequence<Long> task_run_seq = sequence("task_run_seq", SQLDataType.BIGINT);
    }

    interface table {
        Table<Record> dag = table("dag");
        Table<Record> task = table("task");
        Table<Record> dag_run = table("dag_run");
        Table<Record> task_run = table("task_run");
        Table<Record> dag_schedule = table("dag_schedule");
    }

    interface field {
        Field<Long> id = field("id", SQLDataType.BIGINT.nullable(false));
        Field<Long> dag_id = field("dag_id", SQLDataType.BIGINT.nullable(false));
        Field<Long> dag_run_id = field("dag_run_id", SQLDataType.BIGINT.nullable(false));
        Field<Long> task_id = field("task_id", SQLDataType.BIGINT.nullable(false));

        Field<Boolean> latest = field("latest", SQLDataType.BOOLEAN.nullable(false));
        Field<Boolean> enabled = field("enabled", SQLDataType.BOOLEAN.nullable(false));

        Field<Integer> version = field("version", SQLDataType.INTEGER.nullable(false));
        Field<Integer> retry = field("retry", SQLDataType.INTEGER.nullable(false));

        Field<Timestamp> created_date = field("created_date", SQLDataType.TIMESTAMP.nullable(false));
        Field<Timestamp> start_date = field("start_date", SQLDataType.TIMESTAMP.nullable(true));
        Field<Timestamp> finish_date = field("finish_date", SQLDataType.TIMESTAMP.nullable(true));

        Field<String> token = field("token", SQLDataType.VARCHAR(256).nullable(false));
        Field<String> operation = field("operation", SQLDataType.VARCHAR(256).nullable(false));
        Field<String> cron_expression = field("cron_expression", SQLDataType.VARCHAR(32).nullable(true));

        Field<String> status = field("status", SQLDataType.VARCHAR(32).nullable(false));
        Field<String> operation_status = field("operation_status", SQLDataType.VARCHAR(32));

        Field<String> depends = field("depends", SQLDataType.CLOB.nullable(false));
        Field<String> operation_config = field("operation_config", SQLDataType.CLOB.nullable(false));
        Field<String> operation_environment = field("operation_environment", SQLDataType.CLOB.nullable(false));

        Field<String> operation_output = field("operation_output", SQLDataType.CLOB);
    }

}
