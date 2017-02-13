package org.github.mitallast.taskflow.scheduler.command;

import org.github.mitallast.taskflow.dag.TaskRun;

public abstract class TaskRunCommand implements Command {
    private final TaskRun taskRun;

    protected TaskRunCommand(TaskRun taskRun) {
        this.taskRun = taskRun;
    }

    public TaskRun taskRun() {
        return taskRun;
    }
}
