package org.github.mitallast.taskflow.executor.command;

import org.github.mitallast.taskflow.dag.DagRun;

public class StartDagRunCommand extends DagRunCommand {
    public StartDagRunCommand(DagRun dagRun) {
        super(dagRun);
    }
}
