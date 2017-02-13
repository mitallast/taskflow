package org.github.mitallast.taskflow.scheduler.command;

import org.github.mitallast.taskflow.dag.TaskRun;

public class CancelTaskRunCommand extends TaskRunCommand {
    public CancelTaskRunCommand(TaskRun taskRun) {
        super(taskRun);
    }
}
