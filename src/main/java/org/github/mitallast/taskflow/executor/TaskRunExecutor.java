package org.github.mitallast.taskflow.executor;

import org.github.mitallast.taskflow.dag.DagRun;
import org.github.mitallast.taskflow.dag.TaskRun;

public interface TaskRunExecutor {

    void cancel(TaskRun taskRun);

    void schedule(DagRun dagRun, TaskRun taskRun);
}
