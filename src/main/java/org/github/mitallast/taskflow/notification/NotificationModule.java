package org.github.mitallast.taskflow.notification;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

public class NotificationModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(NotificationService.class).asEagerSingleton();

        Multibinder<NotificationProvider> binder = Multibinder.newSetBinder(binder(), NotificationProvider.class);
        binder.addBinding().to(STMPEmailNotificationProvider.class).asEagerSingleton();
    }
}
