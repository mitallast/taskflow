package org.github.mitallast.taskflow.executor;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.github.mitallast.taskflow.common.EventBus;
import org.github.mitallast.taskflow.executor.event.DagRunEvent;

public class ExecutorModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DagRunProcessor.class).asEagerSingleton();
        bind(DagRunExecutor.class).to(DefaultDagRunExecutor.class).asEagerSingleton();
        bind(TaskRunExecutor.class).to(DefaultTaskRunExecutor.class).asEagerSingleton();

        bind(new TypeLiteral<EventBus<DagRunEvent>>() {}).toInstance(new EventBus<>());
    }
}
