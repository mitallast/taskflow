package org.github.mitallast.taskflow.executor.command;

import org.github.mitallast.taskflow.dag.DagRun;

public abstract class DagRunCommand implements Command {

    private final DagRun dagRun;

    protected DagRunCommand(DagRun dagRun) {
        this.dagRun = dagRun;
    }

    public DagRun dagRun() {
        return dagRun;
    }
}
