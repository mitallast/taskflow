package org.github.mitallast.taskflow.operation;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;

import java.io.IOException;

/**
 * Represents abstract operation, can be included as Task into Dag.
 * <p>
 * Implementation must be immutable.
 */
public interface Operation {

    String id();

    Config reference();

    ConfigList schema();

    OperationResult run(OperationCommand command, OperationContext context) throws IOException, InterruptedException;
}
