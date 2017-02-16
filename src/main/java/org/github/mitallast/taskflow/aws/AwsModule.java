package org.github.mitallast.taskflow.aws;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.github.mitallast.taskflow.aws.operation.s3.AwsS3Monitor;
import org.github.mitallast.taskflow.operation.Operation;

public class AwsModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AwsService.class).asEagerSingleton();

        Multibinder<Operation> binder = Multibinder.newSetBinder(binder(), Operation.class);
        binder.addBinding().to(AwsS3Monitor.class).asEagerSingleton();
    }
}
