package org.github.mitallast.taskflow.executor.event;

public abstract class DagRunEvent {
    private final String type = getClass().getSimpleName();
}
