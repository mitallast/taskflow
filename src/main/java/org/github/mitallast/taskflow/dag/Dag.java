package org.github.mitallast.taskflow.dag;

import com.google.common.collect.ImmutableList;

/**
 * Directed Acyclic Graph of tasks
 */
public class Dag {
    private final long id;
    private final int version;
    private final String token;
    private final ImmutableList<Task> tasks;

    public Dag(String token, Task... tasks) {
        this(token, ImmutableList.copyOf(tasks));
    }

    public Dag(String token, ImmutableList<Task> tasks) {
        this(0, 0, token, tasks);
    }

    public Dag(long id, int version, String token, ImmutableList<Task> tasks) {
        this.id = id;
        this.version = version;
        this.token = token;
        this.tasks = tasks;
    }

    public long id() {
        return id;
    }

    public int version() {
        return version;
    }

    public String token() {
        return token;
    }

    public ImmutableList<Task> tasks() {
        return tasks;
    }

    @Override
    public String toString() {
        return "Dag{" +
            "id=" + id +
            ", version=" + version +
            ", token=" + token +
            ", tasks=" + tasks +
            '}';
    }
}
