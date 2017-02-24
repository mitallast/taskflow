package org.github.mitallast.taskflow.docker;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.github.mitallast.taskflow.docker.operation.container.*;
import org.github.mitallast.taskflow.operation.Operation;

public class DockerModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DockerService.class).asEagerSingleton();

        Multibinder<Operation> binder = Multibinder.newSetBinder(binder(), Operation.class);
        binder.addBinding().to(DockerContainerCreate.class).asEagerSingleton();
        binder.addBinding().to(DockerContainerStart.class).asEagerSingleton();
        binder.addBinding().to(DockerContainerStop.class).asEagerSingleton();
        binder.addBinding().to(DockerContainerKill.class).asEagerSingleton();
        binder.addBinding().to(DockerContainerRemove.class).asEagerSingleton();
    }
}
