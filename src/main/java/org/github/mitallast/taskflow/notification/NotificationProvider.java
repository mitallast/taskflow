package org.github.mitallast.taskflow.notification;

public interface NotificationProvider {

    String id();

    void notify(Notification notification);
}
