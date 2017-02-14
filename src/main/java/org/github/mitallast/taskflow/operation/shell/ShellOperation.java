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
    public OperationResult run(OperationCommand command) throws IOException, InterruptedException {

        Config config = command.config().withFallback(reference());

        String charset = config.getString("charset");
        logger.info("charset: {}", charset);

        String path = config.getString("directory");
        File directory = new File(path).getAbsoluteFile();
        logger.info("directory: {}", directory);

        List<String> cmd = new ArrayList<>();
        cmd.add(config.getString("command"));
        cmd.addAll(config.getStringList("args"));

        long timeout = config.getDuration("timeout", TimeUnit.MILLISECONDS);

        ProcessBuilder builder = new ProcessBuilder(cmd);
        builder.environment().putAll(command.environment().map());
        builder.directory(directory);

        try {
            Process process = builder.start();

            boolean exited = false;
            InterruptedException interruptedException = null;
            while (!exited) {
                try {
                    exited = process.waitFor(timeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    interruptedException = e;
                    Thread.currentThread().interrupt();
                }
                if (!exited) {
                    logger.warn("process does not exited, kill");
                    process.destroyForcibly();
                }
            }

            if (interruptedException != null) {
                throw interruptedException;
            }

            int exitValue = process.exitValue();

            String stdout = IOUtils.toString(process.getInputStream(), charset);
            String stderr = IOUtils.toString(process.getErrorStream(), charset);

            return new OperationResult(
                exitValue == 0 ? OperationStatus.SUCCESS : OperationStatus.FAILED,
                stdout,
                stderr
            );
        } catch (IOException e) {
            return new OperationResult(
                OperationStatus.FAILED,
                "",
                e.getMessage()
            );
        }
    }
}
