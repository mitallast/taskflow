package org.github.mitallast.taskflow.notification;

public class Notification {
    private final String subject;
    private final String text;


    public Notification(String subject, String text) {
        this.subject = subject;
        this.text = text;
    }

    public String subject() {
        return subject;
    }

    public String text() {
        return text;
    }
}
