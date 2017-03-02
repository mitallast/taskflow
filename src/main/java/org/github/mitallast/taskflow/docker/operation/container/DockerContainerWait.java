package org.github.mitallast.taskflow.docker.operation.container;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.DockerClientException;
import com.github.dockerjava.api.exception.NotModifiedException;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.command.WaitContainerResultCallback;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.docker.DockerService;
import org.github.mitallast.taskflow.operation.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DockerContainerWait extends AbstractComponent implements Operation {
    private final DockerService dockerService;

    @Inject
    public DockerContainerWait(Config config, DockerService dockerService) {
        super(config.getConfig("operation.docker.container.wait"), DockerContainerWait.class);
        this.dockerService = dockerService;
    }

    @Override
    public String id() {
        return "docker-container-wait";
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
        long timeout = config.getDuration("timeout", TimeUnit.MILLISECONDS);
        long start = System.currentTimeMillis();

        logger.info("filters: {}", filters);
        logger.info("timeout: {}ms", timeout);
        List<Container> containers = dockerService.containers(filters);

        StringBuilder output = new StringBuilder();
        for (Container container : containers) {
            try {
                long left = System.currentTimeMillis() - start;
                logger.info("left: {}ms", left);
                long await = Math.max(1000, timeout - left);
                logger.info("await: {}ms", await);

                Integer statusCode = docker.waitContainerCmd(container.getId())
                    .exec(new WaitContainerResultCallback())
                    .awaitStatusCode(await, TimeUnit.MILLISECONDS);

                output.append("container: ")
                    .append(container.getId())
                    .append(" status code: ")
                    .append(statusCode)
                    .append('\n');
            } catch (DockerClientException e) {
                output.append("timeout: ")
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
