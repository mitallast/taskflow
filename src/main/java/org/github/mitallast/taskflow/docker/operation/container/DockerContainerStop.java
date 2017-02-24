package org.github.mitallast.taskflow.docker.operation.container;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.docker.DockerService;
import org.github.mitallast.taskflow.operation.Operation;
import org.github.mitallast.taskflow.operation.OperationCommand;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.github.mitallast.taskflow.operation.OperationStatus;

import java.io.IOException;
import java.util.List;

public class DockerContainerStop extends AbstractComponent implements Operation {
    private final DockerService dockerService;

    @Inject
    public DockerContainerStop(Config config, DockerService dockerService) {
        super(config.getConfig("operation.docker.container.stop"), DockerContainerStop.class);
        this.dockerService = dockerService;
    }

    @Override
    public String id() {
        return "docker-container-stop";
    }

    @Override
    public Config reference() {
        return config.getConfig("reference");
    }

    @Override
    public Config schema() {
        return config.getConfig("schema");
    }

    @Override
    public OperationResult run(OperationCommand command) throws IOException, InterruptedException {
        DockerClient docker = dockerService.docker();
        Config config = command.config().withFallback(reference());

        String filter = config.getString("filter");

        List<Container> containers = docker.listContainersCmd()
            .withLabelFilter(filter)
            .withShowAll(true)
            .exec();

        for (Container container : containers) {
            docker.stopContainerCmd(container.getId()).exec();
        }

        return new OperationResult(
            OperationStatus.SUCCESS,
            "containers stopped: " + containers
        );
    }
}
