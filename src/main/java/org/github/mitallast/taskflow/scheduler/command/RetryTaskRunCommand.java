package org.github.mitallast.taskflow.scheduler.command;

import org.github.mitallast.taskflow.dag.TaskRun;

public class RetryTaskRunCommand extends TaskRunCommand {
    public RetryTaskRunCommand(TaskRun taskRun) {
        super(taskRun);
    }
}
