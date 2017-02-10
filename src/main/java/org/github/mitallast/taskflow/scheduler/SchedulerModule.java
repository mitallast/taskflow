package org.github.mitallast.taskflow.scheduler;

import com.google.inject.AbstractModule;

public class SchedulerModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DagScheduler.class).asEagerSingleton();
    }
}
