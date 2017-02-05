package org.github.mitallast.taskflow.operation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

/**
 * Represents context for operation.
 */
public class OperationCommand {
    private final Config config;
    private final OperationEnvironment environment;

    @JsonCreator
    public OperationCommand(
        @JsonProperty("config") Config config,
        @JsonProperty("environment") OperationEnvironment environment
    ) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(environment);
        this.config = config;
        this.environment = environment;
    }

    public Config config() {
        return config;
    }

    public OperationEnvironment environment() {
        return environment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OperationCommand that = (OperationCommand) o;

        if (!config.equals(that.config)) return false;
        return environment.equals(that.environment);
    }

    @Override
    public int hashCode() {
        int result = config.hashCode();
        result = 31 * result + environment.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "OperationCommand{" +
            "config=" + config.root().render(ConfigRenderOptions.concise()) +
            ", environment=" + environment +
            '}';
    }
}
