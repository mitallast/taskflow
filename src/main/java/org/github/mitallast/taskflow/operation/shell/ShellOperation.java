package org.github.mitallast.taskflow.operation.shell;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.IOUtils;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.operation.Operation;
import org.github.mitallast.taskflow.operation.OperationCommand;
import org.github.mitallast.taskflow.operation.OperationResult;
import org.github.mitallast.taskflow.operation.OperationStatus;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ShellOperation extends AbstractComponent implements Operation {

    @Inject
    public ShellOperation(Config config) {
        super(config.getConfig("operation.shell"), ShellOperation.class);
    }

    @Override
    public String id() {
        return "shell";
    }

    @Override
    public Config reference() {
        return config;
    }

    @Override
    public OperationResult run(OperationCommand command) throws IOException {

        Config config = command.config().withFallback(reference());

        String charset = config.getString("charset");
        logger.info("charset: {}", charset);

        String path = config.getString("directory");
        File directory = new File(path);
        logger.info("directory: {}", directory.getAbsolutePath());

        List<String> cmd = new ArrayList<>();
        cmd.add(config.getString("command"));
        cmd.addAll(config.getStringList("args"));

        long timeout = config.getDuration("timeout", TimeUnit.MILLISECONDS);

        ProcessBuilder builder = new ProcessBuilder(cmd);
        builder.environment().putAll(command.environment().map());
        builder.directory(directory);

        try {
            Process process = builder.start();

            boolean exited = process.waitFor(timeout, TimeUnit.MILLISECONDS);
            final int exitValue;
            if (exited) {
                exitValue = process.exitValue();
            } else {
                process.destroy();
                exitValue = process.waitFor();
            }

            String stdout = IOUtils.toString(process.getInputStream(), charset);
            String stderr = IOUtils.toString(process.getErrorStream(), charset);

            return new OperationResult(
                exitValue == 0 ? OperationStatus.SUCCESS : OperationStatus.FAILURE,
                stdout,
                stderr
            );
        } catch (IOException e) {
            return new OperationResult(
                OperationStatus.FAILURE,
                "",
                e.getMessage()
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e.getCause());
        }
    }
}
