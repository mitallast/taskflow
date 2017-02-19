package org.github.mitallast.taskflow.executor;

import com.google.inject.AbstractModule;

public class ExecutorModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DagRunProcessor.class).asEagerSingleton();
        bind(DagRunExecutor.class).asEagerSingleton();
        bind(TaskRunExecutor.class).asEagerSingleton();
    }
}
