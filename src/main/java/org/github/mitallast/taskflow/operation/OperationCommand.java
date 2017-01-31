package org.github.mitallast.taskflow.operation;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

/**
 * Represents context for operation.
 */
public class OperationCommand {
    private final Config config;
    private final OperationEnvironment environment;

    public OperationCommand(Config config, OperationEnvironment environment) {
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
    public String toString() {
        return "OperationCommand{" +
            "config=" + config.root().render(ConfigRenderOptions.concise()) +
            ", environment=" + environment +
            '}';
    }
}
