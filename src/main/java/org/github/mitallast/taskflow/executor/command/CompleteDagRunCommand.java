package org.github.mitallast.taskflow.executor.command;

import org.github.mitallast.taskflow.dag.DagRun;

public class CompleteDagRunCommand extends DagRunCommand {
    public CompleteDagRunCommand(DagRun dagRun) {
        super(dagRun);
    }
}
