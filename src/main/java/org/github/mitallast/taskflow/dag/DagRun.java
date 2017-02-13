package org.github.mitallast.taskflow.dag;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;

import static org.github.mitallast.taskflow.common.Immutable.replace;
import static org.github.mitallast.taskflow.common.Immutable.append;

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

    public DagRun start() {
        Preconditions.checkArgument(status == DagRunStatus.PENDING);
        Preconditions.checkArgument(startDate == null);
        Preconditions.checkArgument(finishDate == null);
        return new DagRun(id, dagId, createdDate, new DateTime(), null, DagRunStatus.RUNNING, tasks);
    }

    public DagRun start(TaskRun... taskRuns) {
        DagRun dagRun = this;
        for (TaskRun taskRun : taskRuns) {
            dagRun = dagRun.update(taskRun.start());
        }
        return dagRun;
    }

    public DagRun success(TaskRun... taskRuns) {
        DagRun dagRun = this;
        for (TaskRun taskRun : taskRuns) {
            dagRun = dagRun.update(taskRun.start().success());
        }
        return dagRun;
    }

    public DagRun failure(TaskRun... taskRuns) {
        DagRun dagRun = this;
        for (TaskRun taskRun : taskRuns) {
            dagRun = dagRun.update(taskRun.start().failure());
        }
        return dagRun;
    }

    public DagRun cancel(TaskRun... taskRuns) {
        DagRun dagRun = this;
        for (TaskRun taskRun : taskRuns) {
            dagRun = dagRun.update(taskRun.start().cancel());
        }
        return dagRun;
    }

    public DagRun success() {
        Preconditions.checkArgument(status == DagRunStatus.RUNNING);
        Preconditions.checkNotNull(startDate);
        Preconditions.checkArgument(finishDate == null);
        return new DagRun(id, dagId, createdDate, startDate, new DateTime(), DagRunStatus.SUCCESS, tasks);
    }

    public DagRun update(TaskRun taskRun) {
        Preconditions.checkNotNull(taskRun);
        Preconditions.checkArgument(tasks.stream().anyMatch(t -> t.id() == taskRun.id()));
        return new DagRun(id, dagId, createdDate, startDate, finishDate, DagRunStatus.RUNNING, replace(tasks, t -> t.id() == taskRun.id(), taskRun));
    }

    public DagRun retry(TaskRun taskRun) {
        Preconditions.checkNotNull(taskRun);
        Preconditions.checkArgument(tasks.stream().noneMatch(t -> t.id() == taskRun.id()));
        return new DagRun(id, dagId, createdDate, startDate, finishDate, DagRunStatus.RUNNING, append(tasks, taskRun));

    }
}
