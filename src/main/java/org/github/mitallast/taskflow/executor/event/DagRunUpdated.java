package org.github.mitallast.taskflow.executor.event;

import org.github.mitallast.taskflow.dag.DagRun;

public class DagRunUpdated extends DagRunEvent {

    private final DagRun dagRun;

    public DagRunUpdated(DagRun dagRun) {
        this.dagRun = dagRun;
    }
}
