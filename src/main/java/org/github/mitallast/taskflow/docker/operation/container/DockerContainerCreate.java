package org.github.mitallast.taskflow.docker.operation.container;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.exception.ConflictException;
import com.github.dockerjava.api.exception.NotFoundException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.docker.DockerService;
import org.github.mitallast.taskflow.operation.Operation;
import org.github.mitallast.taskflow.operation.OperationCommand;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.github.mitallast.taskflow.operation.OperationStatus;

import java.io.IOException;
import java.util.List;

import static org.github.mitallast.taskflow.common.Immutable.toMap;

public class DockerContainerCreate extends AbstractComponent implements Operation {
    private final DockerService dockerService;

    @Inject
    public DockerContainerCreate(Config config, DockerService dockerService) {
        super(config.getConfig("operation.docker.container.create"), DockerContainerCreate.class);
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
    public ConfigList schema() {
        return config.getList("schema");
    }

    @Override
    public OperationResult run(OperationCommand command) throws IOException, InterruptedException {
        DockerClient docker = dockerService.docker();
        Config config = command.config().withFallback(reference());

        String image = config.getString("image");
        String name = config.getString("name");

        ImmutableMap<String, String> labels = toMap(config.getConfig("labels"));

        logger.info("create container from image {}", image);
        logger.info("container labels {}", labels);

        try {
            CreateContainerResponse exec = docker.createContainerCmd(image)
                .withName(name)
                .withLabels(labels)
                .withEnv(command.environment().list())
                .exec();

            return new OperationResult(
                OperationStatus.SUCCESS,
                "container created: " + exec.getId()
            );
        } catch (NotFoundException e) {
            return new OperationResult(
                OperationStatus.FAILED,
                "container not found: " + e.getMessage()
            );
        } catch (ConflictException e) {
            return new OperationResult(
                OperationStatus.FAILED,
                "container name conflict: " + e.getMessage()
            );
        }
    }
}
