package org.github.mitallast.taskflow.scheduler.command;

import org.github.mitallast.taskflow.dag.DagRun;

public class AwaitCommand extends DagRunCommand {
    public AwaitCommand(DagRun dagRun) {
        super(dagRun);
    }
}
