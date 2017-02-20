package org.github.mitallast.taskflow.persistence;

import com.google.inject.AbstractModule;
import org.github.mitallast.taskflow.dag.DagPersistenceService;

public class PersistenceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(PersistenceService.class).asEagerSingleton();
        bind(SchemaService.class).asEagerSingleton();
        bind(DagPersistenceService.class).to(DefaultDagPersistenceService.class).asEagerSingleton();
    }
}
