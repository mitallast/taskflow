package org.github.mitallast.taskflow.common;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;

public class EventBus<T> {
    private final Logger logger = LogManager.getLogger();
    private final ConcurrentMap<String, CopyOnWriteArraySet<BiConsumer<String, T>>> subscribersMap;

    public EventBus() {
        this.subscribersMap = new ConcurrentHashMap<>();
    }

    public void subscribe(String channel, BiConsumer<String, T> consumer) {
        Preconditions.checkNotNull(channel);
        Preconditions.checkNotNull(consumer);
        subscribersMap
            .computeIfAbsent(channel, s -> new CopyOnWriteArraySet<>())
            .add(consumer);
    }

    public void unsubscribe(String channel, BiConsumer<String, T> consumer) {
        Preconditions.checkNotNull(channel);
        Preconditions.checkNotNull(consumer);
        CopyOnWriteArraySet<BiConsumer<String, T>> consumers = subscribersMap.get(channel);
        if (consumers != null) {
            consumers.remove(consumer);
        }
    }

    public void remove(String channel) {
        Preconditions.checkNotNull(channel);
        subscribersMap.remove(channel);
    }

    public void trigger(String channel, T event) {
        Preconditions.checkNotNull(channel);
        Preconditions.checkNotNull(event);
        CopyOnWriteArraySet<BiConsumer<String, T>> consumers = subscribersMap.get(channel);
        if (consumers != null) {
            for (BiConsumer<String, T> consumer : consumers) {
                try {
                    consumer.accept(channel, event);
                } catch (Exception e) {
                    logger.warn("unexpected exception", e);
                }
            }
        }

    }
}
