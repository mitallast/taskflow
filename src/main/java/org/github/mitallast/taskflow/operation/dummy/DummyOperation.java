package org.github.mitallast.taskflow.operation.dummy;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.operation.*;

import java.io.IOException;

public class DummyOperation extends AbstractComponent implements Operation {

    @Inject
    public DummyOperation(Config config) {
        super(config.getConfig("operation.dummy"), DummyOperation.class);
    }

    @Override
    public String id() {
        return "dummy";
    }

    @Override
    public Config reference() {
        return config.getConfig("reference");
    }

    @Override
    public ConfigList schema() {
        return config.getList("schema");
    }

    @Override
    public OperationResult run(OperationCommand command, OperationContext context) throws IOException, InterruptedException {
        Config config = command.config().withFallback(reference());

        if (config.getString("status").equalsIgnoreCase("success")) {
            return new OperationResult(OperationStatus.SUCCESS, "Success dummy operation");
        } else {
            return new OperationResult(OperationStatus.FAILED, "Failed dummy operation");
        }
    }
}
