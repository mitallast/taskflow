package org.github.mitallast.taskflow.dag;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import static org.github.mitallast.taskflow.common.Immutable.replace;

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

    @JsonCreator
    public Dag(
        @JsonProperty("id") long id,
        @JsonProperty("version") int version,
        @JsonProperty("token") String token,
        @JsonProperty("tasks") ImmutableList<Task> tasks
    ) {
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

    public Dag update(Task task) {
        Preconditions.checkNotNull(task);
        Preconditions.checkArgument(tasks.stream().anyMatch(t -> t.id() == task.id()));
        return new Dag(id, version, token, replace(tasks, t -> t.id() == task.id(), task));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Dag dag = (Dag) o;

        if (id != dag.id) return false;
        if (version != dag.version) return false;
        if (!token.equals(dag.token)) return false;
        return tasks.equals(dag.tasks);
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + version;
        result = 31 * result + token.hashCode();
        result = 31 * result + tasks.hashCode();
        return result;
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
