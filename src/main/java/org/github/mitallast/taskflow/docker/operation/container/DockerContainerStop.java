package org.github.mitallast.taskflow.docker.operation.container;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.exception.NotModifiedException;
import com.github.dockerjava.api.model.Container;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.docker.DockerService;
import org.github.mitallast.taskflow.operation.*;

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
    public ConfigList schema() {
        return config.getList("schema");
    }

    @Override
    public OperationResult run(OperationCommand command, OperationContext context) throws IOException, InterruptedException {
        DockerClient docker = dockerService.docker();
        Config config = command.config().withFallback(reference());
        Config filters = config.getConfig("filters");

        logger.info("filters: {}", filters);
        List<Container> containers = dockerService.containers(filters);

        StringBuilder output = new StringBuilder();
        for (Container container : containers) {
            try {
                docker.stopContainerCmd(container.getId()).exec();
                output.append("container stopped: ")
                    .append(container.getId())
                    .append('\n');
            } catch (NotFoundException e) {
                output.append("container not found: ")
                    .append(container.getId())
                    .append('\n')
                    .append(e.getMessage());
                return new OperationResult(OperationStatus.FAILED, output.toString());
            } catch (NotModifiedException e) {
                output.append("container already stopped: ")
                    .append(container.getId())
                    .append('\n')
                    .append(e.getMessage());
                return new OperationResult(OperationStatus.FAILED, output.toString());
            }
        }
        return new OperationResult(OperationStatus.SUCCESS, output.toString());
    }
}
