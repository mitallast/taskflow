package org.github.mitallast.taskflow.executor;

public interface DagRunExecutor {
    void cancel(long dagRunId);
    void schedule(long dagRunId);
}
