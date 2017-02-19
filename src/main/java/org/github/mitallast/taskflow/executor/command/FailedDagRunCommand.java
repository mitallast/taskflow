package org.github.mitallast.taskflow.executor.command;

import org.github.mitallast.taskflow.dag.DagRun;

public class FailedDagRunCommand extends DagRunCommand {
    public FailedDagRunCommand(DagRun dagRun) {
        super(dagRun);
    }
}
