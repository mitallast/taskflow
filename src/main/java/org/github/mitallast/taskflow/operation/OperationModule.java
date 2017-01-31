package org.github.mitallast.taskflow.operation;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.github.mitallast.taskflow.operation.dummy.DummyOperation;
import org.github.mitallast.taskflow.operation.shell.ShellOperation;

public class OperationModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(OperationService.class).asEagerSingleton();

        Multibinder<Operation> binder = Multibinder.newSetBinder(binder(), Operation.class);
        binder.addBinding().to(DummyOperation.class).asEagerSingleton();
        binder.addBinding().to(ShellOperation.class).asEagerSingleton();
    }
}
