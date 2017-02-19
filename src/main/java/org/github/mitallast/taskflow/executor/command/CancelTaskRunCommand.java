package org.github.mitallast.taskflow.executor.command;

import org.github.mitallast.taskflow.dag.TaskRun;

public class CancelTaskRunCommand extends TaskRunCommand {
    public CancelTaskRunCommand(TaskRun taskRun) {
        super(taskRun);
    }
}
