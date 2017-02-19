package org.github.mitallast.taskflow.executor.command;

import org.github.mitallast.taskflow.dag.DagRun;

public class CancelDagRunCommand extends DagRunCommand {
    public CancelDagRunCommand(DagRun dagRun) {
        super(dagRun);
    }
}
