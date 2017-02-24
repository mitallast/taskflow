package org.github.mitallast.taskflow.docker.operation.container;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
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
import java.util.stream.Collectors;

public class DockerCreateContainer extends AbstractComponent implements Operation {
    private final DockerService dockerService;

    @Inject
    public DockerCreateContainer(Config config, DockerService dockerService) {
        super(config.getConfig("operation.docker.container.create"), DockerCreateContainer.class);
        this.dockerService = dockerService;
    }

    @Override
    public String id() {
        return "docker-container-create";
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

        String image = config.getString("image");
        String name = config.getString("name");

        List<String> env = command.environment().map().entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.toList());

        CreateContainerResponse exec = docker.createContainerCmd(image)
            .withName(name)
            .withEnv(env)
            .exec();

        return new OperationResult(
            OperationStatus.SUCCESS,
            exec.toString()
        );
    }
}
