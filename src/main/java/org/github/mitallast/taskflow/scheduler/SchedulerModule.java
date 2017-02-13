package org.github.mitallast.taskflow.scheduler;

import com.google.inject.AbstractModule;

public class SchedulerModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DagRunScheduler.class).asEagerSingleton();
        bind(DagRunExecutor.class).asEagerSingleton();
        bind(TaskRunExecutor.class).asEagerSingleton();
    }
}
