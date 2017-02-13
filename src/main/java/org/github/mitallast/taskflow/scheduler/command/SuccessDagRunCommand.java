package org.github.mitallast.taskflow.scheduler.command;

import org.github.mitallast.taskflow.dag.DagRun;

public class SuccessDagRunCommand extends DagRunCommand {
    public SuccessDagRunCommand(DagRun dagRun) {
        super(dagRun);
    }
}
