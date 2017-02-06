package org.github.mitallast.taskflow.dag;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.github.mitallast.taskflow.operation.OperationCommand;

/**
 * Parametrized instance of an operator
 */
public class Task {
    private final long id;
    private final int version;
    private final String token;
    private final ImmutableSet<String> depends;
    private final String operation;
    private final OperationCommand command;

    public Task(String token, ImmutableSet<String> depends, String operation, OperationCommand command) {
        this(0, 0, token, depends, operation, command);
    }

    @JsonCreator
    public Task(
        @JsonProperty("id") long id,
        @JsonProperty("version") int version,
        @JsonProperty("token") String token,
        @JsonProperty("depends") ImmutableSet<String> depends,
        @JsonProperty("operation") String operation,
        @JsonProperty("command") OperationCommand command) {
        this.id = id;
        this.version = version;
        this.token = token;
        this.depends = depends;
        this.operation = operation;
        this.command = command;
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

    public ImmutableSet<String> depends() {
        return depends;
    }

    public String operation() {
        return operation;
    }

    public OperationCommand command() {
        return command;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Task task = (Task) o;

        if (id != task.id) return false;
        if (version != task.version) return false;
        if (!token.equals(task.token)) return false;
        if (!depends.equals(task.depends)) return false;
        if (!operation.equals(task.operation)) return false;
        return command.equals(task.command);
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + version;
        result = 31 * result + token.hashCode();
        result = 31 * result + depends.hashCode();
        result = 31 * result + operation.hashCode();
        result = 31 * result + command.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Task{" +
            "id=" + id +
            ", version=" + version +
            ", token=" + token +
            ", depends=" + depends +
            ", operation=" + operation +
            ", command=" + command +
            '}';
    }
}