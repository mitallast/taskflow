package org.github.mitallast.taskflow.executor.event;

import org.github.mitallast.taskflow.dag.DagRun;
import org.github.mitallast.taskflow.dag.DagRunStatus;
import org.joda.time.DateTime;

public class DagRunStatusUpdated extends DagRunEvent {
    private final DagRunStatus status;
    private final DateTime startDate;
    private final DateTime finishDate;

    public DagRunStatusUpdated(DagRun dagRun) {
        status = dagRun.status();
        startDate = dagRun.startDate();
        finishDate = dagRun.finishDate();
    }
}
