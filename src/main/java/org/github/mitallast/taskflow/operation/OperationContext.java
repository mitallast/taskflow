package org.github.mitallast.taskflow.operation;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class OperationContext {
    private final ExecutorService executionContext;
    private final Consumer<String> outputListener;

    public OperationContext(ExecutorService executionContext, Consumer<String> outputListener) {
        this.executionContext = executionContext;
        this.outputListener = outputListener;
    }

    public ExecutorService executionContext() {
        return executionContext;
    }

    public Consumer<String> outputListener() {
        return outputListener;
    }
}
