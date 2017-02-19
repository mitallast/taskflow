package org.github.mitallast.taskflow.executor.command;

import org.github.mitallast.taskflow.dag.TaskRun;

public class ExecuteTaskRunCommand extends TaskRunCommand {
    public ExecuteTaskRunCommand(TaskRun taskRun) {
        super(taskRun);
    }
}
