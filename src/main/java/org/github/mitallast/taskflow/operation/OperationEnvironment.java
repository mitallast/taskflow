package org.github.mitallast.taskflow.operation;

import com.google.common.collect.ImmutableMap;

import java.util.Optional;

/**
 * Operation environment
 */
public class OperationEnvironment {
    private final ImmutableMap<String, String> env;

    public OperationEnvironment() {
        this(ImmutableMap.of());
    }

    public OperationEnvironment(ImmutableMap<String, String> env) {
        this.env = env;
    }

    public Optional<String> get(String key) {
        return Optional.of(env.get(key));
    }

    public OperationEnvironment with(String key, String value) {
        ImmutableMap<String, String> map = ImmutableMap.<String, String>builder().putAll(env).put(key, value).build();
        return new OperationEnvironment(map);
    }

    public ImmutableMap<String, String> map() {
        return env;
    }

    @Override
    public String toString() {
        return env.toString();
    }
}
