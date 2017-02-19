package org.github.mitallast.taskflow.executor.command;

import org.github.mitallast.taskflow.dag.TaskRun;

public class RetryTaskRunCommand extends TaskRunCommand {
    public RetryTaskRunCommand(TaskRun taskRun) {
        super(taskRun);
    }
}
