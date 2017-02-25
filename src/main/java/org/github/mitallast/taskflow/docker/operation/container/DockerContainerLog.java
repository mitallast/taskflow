package org.github.mitallast.taskflow.docker.operation.container;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.async.ResultCallbackTemplate;
import com.github.dockerjava.core.command.LogContainerResultCallback;
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
import java.util.concurrent.TimeUnit;

public class DockerContainerLog extends AbstractComponent implements Operation {
    private final DockerService dockerService;

    @Inject
    public DockerContainerLog(Config config, DockerService dockerService) {
        super(config.getConfig("operation.docker.container.log"), DockerContainerLog.class);
        this.dockerService = dockerService;
    }

    @Override
    public String id() {
        return "docker-container-log";
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

        Config filters = config.getConfig("filters");
        long timeout = config.getDuration("timeout", TimeUnit.MILLISECONDS);
        long start = System.currentTimeMillis();

        logger.info("filters: {}", filters);
        logger.info("timeout: {}ms", timeout);

        List<Container> containers = dockerService.containers(filters);

        StringBuilder output = new StringBuilder();
        for (Container container : containers) {
            LogContainerCallback loggingCallback = new LogContainerCallback();

            try {
                long left = System.currentTimeMillis() - start;
                logger.info("left: {}ms", left);
                long await = Math.max(1000, timeout - left);
                logger.info("await: {}ms", await);

                docker.logContainerCmd(container.getId())
                    .withStdOut(true)
                    .withStdErr(true)
                    .withTailAll()
                    .exec(loggingCallback);

                loggingCallback.awaitCompletion(await, TimeUnit.MILLISECONDS);

                output.append("container log: ")
                    .append(container.getId())
                    .append('\n')
                    .append(loggingCallback.builder)
                    .append("\n");
            } catch (NotFoundException e) {
                output.append("container not found: ")
                    .append(container.getId())
                    .append('\n')
                    .append(e.getMessage());
                return new OperationResult(OperationStatus.FAILED, output.toString());
            }
        }
        return new OperationResult(OperationStatus.SUCCESS, output.toString());
    }

    public class LogContainerCallback extends ResultCallbackTemplate<LogContainerResultCallback, Frame> {

        private StringBuilder builder = new StringBuilder();

        @Override
        public void onNext(Frame item) {
            builder.append(item.toString()).append('\n');
        }
    }
}