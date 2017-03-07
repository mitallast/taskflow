package org.github.mitallast.taskflow.operation.shell;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import org.github.mitallast.taskflow.common.IOUtils;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.operation.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
        return config.getConfig("reference");
    }

    @Override
    public ConfigList schema() {
        return config.getList("schema");
    }

    @Override
    public OperationResult run(OperationCommand command, OperationContext context) throws IOException, InterruptedException {

        Config config = command.config().withFallback(reference());

        String path = config.getString("directory");
        File directory = new File(path).getAbsoluteFile();
        logger.info("directory: {}", directory);

        String script = config.getString("script");

        long timeout = config.getDuration("timeout", TimeUnit.MILLISECONDS);

        File scriptFile = null;
        try {
            scriptFile = File.createTempFile("taskflow", ".sh");
            IOUtils.write(scriptFile, script);
            if (!scriptFile.setExecutable(true)) {
                logger.warn("error set executable {}", scriptFile);
            }

            ProcessBuilder builder = new ProcessBuilder(scriptFile.getAbsolutePath());
            builder.redirectErrorStream(true);
            builder.environment().putAll(command.environment().map());
            builder.directory(directory);

            final Process process = builder.start();
            final CompletableFuture<String> output = readStream(process.getInputStream(), context);

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
            logger.info("exit code: {}", exitValue);

            return new OperationResult(
                exitValue == 0 ? OperationStatus.SUCCESS : OperationStatus.FAILED,
                output.get()
            );
        } catch (IOException | ExecutionException e) {
            logger.info("operation failed", e);
            return new OperationResult(
                OperationStatus.FAILED,
                e.getMessage()
            );
        } finally {
            if (scriptFile != null) {
                Files.deleteIfExists(scriptFile.toPath());
            }
        }
    }

    private CompletableFuture<String> readStream(InputStream inputStream, OperationContext context) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StringBuilder output = new StringBuilder();
                new BufferedReader(new InputStreamReader(inputStream))
                    .lines()
                    .forEach(string -> {
                        logger.info(string);
                        String line = string + '\n';
                        output.append(line);
                        context.outputListener().accept(line);
                    });
                return output.toString();
            } catch (Exception e) {
                logger.warn("unexpected exception", e);
                return "";
            }
        }, context.executionContext());
    }
}
