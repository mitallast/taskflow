package org.github.mitallast.taskflow.dag;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;

public class DagRun {
    private final long id;
    private final long dagId;
    private final DateTime createdDate;
    private final DateTime startDate;
    private final DateTime finishDate;
    private final DagRunStatus status;
    private final ImmutableList<TaskRun> tasks;

    public DagRun(long id, long dagId, DateTime createdDate, DateTime startDate, DateTime finishDate, DagRunStatus status, ImmutableList<TaskRun> tasks) {
        this.id = id;
        this.dagId = dagId;
        this.createdDate = createdDate;
        this.startDate = startDate;
        this.finishDate = finishDate;
        this.status = status;
        this.tasks = tasks;
    }

    public long id() {
        return id;
    }

    public long dagId() {
        return dagId;
    }

    public DateTime createdDate() {
        return createdDate;
    }

    public DateTime startDate() {
        return startDate;
    }

    public DateTime finishDate() {
        return finishDate;
    }

    public DagRunStatus status() {
        return status;
    }

    public ImmutableList<TaskRun> tasks() {
        return tasks;
    }
}
