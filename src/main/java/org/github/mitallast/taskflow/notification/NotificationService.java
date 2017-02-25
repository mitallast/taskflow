package org.github.mitallast.taskflow.notification;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;

import java.util.Set;

import static org.github.mitallast.taskflow.common.Immutable.group;

public class NotificationService extends AbstractComponent {

    private final ImmutableMap<String, NotificationProvider> providersMap;

    @Inject
    public NotificationService(Config config, Set<NotificationProvider> providers) {
        super(config, NotificationService.class);

        providersMap = group(providers, NotificationProvider::id);
    }

    public ImmutableCollection<NotificationProvider> providers() {
        return providersMap.values();
    }

    public void notify(Notification notification) {
        for (NotificationProvider notificationProvider : providersMap.values()) {
            notificationProvider.notify(notification);
        }
    }
}
