package org.github.mitallast.taskflow.dag;

import com.google.inject.AbstractModule;

public class DagModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DagPersistenceService.class).asEagerSingleton();
        bind(DagService.class).asEagerSingleton();
    }
}
