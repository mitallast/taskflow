package org.github.mitallast.taskflow.dag;

import com.google.common.collect.ImmutableList;
import org.github.mitallast.taskflow.operation.OperationCommand;

/**
 * Parametrized instance of an operator
 */
public class Task {
    private final long id;
    private final int version;
    private final String token;
    private final ImmutableList<String> depends;
    private final String operation;
    private final OperationCommand command;

    public Task(String token, ImmutableList<String> depends, String operation, OperationCommand command) {
        this(0, 0, token, depends, operation, command);
    }

    public Task(long id, int version, String token, ImmutableList<String> depends, String operation, OperationCommand command) {
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

    public ImmutableList<String> depends() {
        return depends;
    }

    public String operation() {
        return operation;
    }

    public OperationCommand command() {
        return command;
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