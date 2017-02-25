package org.github.mitallast.taskflow.notification;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.github.mitallast.taskflow.common.component.AbstractComponent;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Map;
import java.util.Properties;

import static org.github.mitallast.taskflow.common.Immutable.toMap;

public class STMPEmailNotificationProvider extends AbstractComponent implements NotificationProvider {

    private final Properties properties;
    private final Session session;

    private final String sendFrom;
    private final String sendTo;

    @Inject
    public STMPEmailNotificationProvider(Config config) {
        super(config, STMPEmailNotificationProvider.class);

        sendFrom = config.getString("notification.mail.sendFrom");
        sendTo = config.getString("notification.mail.sendTo");

        properties = new Properties();
        for (Map.Entry<String, String> entry : toMap(config.getConfig("notification.mail.properties")).entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }

        logger.info("properties: {}", properties);

        session = Session.getDefaultInstance(properties, null);
        session.setDebug(true);
    }

    @Override
    public String id() {
        return "email";
    }

    @Override
    public void notify(Notification notification) {
        try {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(sendFrom));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(sendTo));
            message.setSubject(notification.subject());
            message.setText(notification.text());

            Transport transport = session.getTransport();
            transport.connect();
            transport.sendMessage(message, message.getAllRecipients());
        } catch (MessagingException e) {
            logger.warn("error send mail", e);
        }
    }
}
