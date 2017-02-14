package org.github.mitallast.taskflow.operation;

import com.typesafe.config.Config;

import java.io.IOException;

/**
 * Represents abstract operation, can be included as Task into Dag.
 * <p>
 * Implementation must be immutable.
 */
public interface Operation {

    String id();

    Config reference();

    OperationResult run(OperationCommand command) throws IOException, InterruptedException;
}
