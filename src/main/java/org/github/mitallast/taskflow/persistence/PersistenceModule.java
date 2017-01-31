package org.github.mitallast.taskflow.persistence;

import com.google.inject.AbstractModule;

public class PersistenceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(PersistenceService.class).asEagerSingleton();
    }
}
