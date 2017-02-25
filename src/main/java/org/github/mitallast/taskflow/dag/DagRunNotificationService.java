package org.github.mitallast.taskflow.dag;

import com.google.inject.Inject;
import org.github.mitallast.taskflow.notification.Notification;
import org.github.mitallast.taskflow.notification.NotificationService;
import org.github.mitallast.taskflow.operation.OperationResult;

public class DagRunNotificationService {
    private final NotificationService notificationService;

    @Inject
    public DagRunNotificationService(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    public void sendTaskFailed(DagRun dagRun, TaskRun taskRun, OperationResult operationResult) {
        StringBuilder builder = new StringBuilder();
        render(builder, operationResult);
        render(builder, taskRun);
        render(builder, dagRun);
        Notification notification = new Notification(
            "Dag run " + dagRun.id() + ": task run " + taskRun.id() + " failed",
            builder.toString()
        );
        notificationService.notify(notification);
    }

    public void sendDagFailed(DagRun dagRun) {
        StringBuilder builder = new StringBuilder();
        render(builder, dagRun);
        for (TaskRun taskRun : dagRun.tasks()) {
            render(builder, taskRun);
        }

        Notification notification = new Notification(
            "Dag run " + dagRun.id() + " failed",
            builder.toString()
        );
        notificationService.notify(notification);
    }

    public void sendDagCanceled(DagRun dagRun) {
        StringBuilder builder = new StringBuilder();
        render(builder, dagRun);
        for (TaskRun taskRun : dagRun.tasks()) {
            render(builder, taskRun);
        }

        Notification notification = new Notification(
            "Dag run " + dagRun.id() + " canceled",
            builder.toString()
        );
        notificationService.notify(notification);
    }

    public void sendDagSuccess(DagRun dagRun) {
        StringBuilder builder = new StringBuilder();
        render(builder, dagRun);
        for (TaskRun taskRun : dagRun.tasks()) {
            render(builder, taskRun);
        }

        Notification notification = new Notification(
            "Dag run " + dagRun.id() + " success",
            builder.toString()
        );
        notificationService.notify(notification);
    }

    private void render(StringBuilder builder, TaskRun taskRun) {
        builder.append("Task run:\r\n")
            .append("id: ").append(taskRun.id()).append("\r\n")
            .append("task: ").append(taskRun.task().token()).append(":").append(taskRun.task().version()).append("\r\n")
            .append("created: ").append(taskRun.createdDate()).append("\r\n")
            .append("started: ").append(taskRun.startDate()).append("\r\n")
            .append("finished: ").append(taskRun.finishDate()).append("\r\n")
            .append("status: ").append(taskRun.status()).append("\r\n")
            .append("\r\n");
    }

    private void render(StringBuilder builder, DagRun dagRun) {
        builder.append("Dag run:\r\n")
            .append("id: ").append(dagRun.id()).append("\r\n")
            .append("dag: ").append(dagRun.dag().token()).append(":").append(dagRun.dag().version()).append("\r\n")
            .append("created: ").append(dagRun.createdDate()).append("\r\n")
            .append("started: ").append(dagRun.startDate()).append("\r\n")
            .append("finished: ").append(dagRun.finishDate()).append("\r\n")
            .append("status: ").append(dagRun.status()).append("\r\n");
    }

    private void render(StringBuilder builder, OperationResult taskRun) {
        builder.append("Operation result:\r\n")
            .append("status: ").append(taskRun.status()).append("\r\n")
            .append("output:\r\n").append(taskRun.status()).append("\r\n");
    }
}

