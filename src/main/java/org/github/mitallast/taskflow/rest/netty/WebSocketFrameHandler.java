package org.github.mitallast.taskflow.rest.netty;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.AttributeKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.github.mitallast.taskflow.common.EventBus;
import org.github.mitallast.taskflow.common.json.JsonService;
import org.github.mitallast.taskflow.executor.event.DagRunEvent;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;

@ChannelHandler.Sharable
public class WebSocketFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

    private final static AttributeKey<ChannelConsumer> consumerAttr = AttributeKey.newInstance("consumer");

    private final Logger logger = LogManager.getLogger();
    private final JsonService jsonService;
    private final EventBus<DagRunEvent> eventBus;

    @Inject
    public WebSocketFrameHandler(JsonService jsonService, EventBus<DagRunEvent> eventBus) {
        this.jsonService = jsonService;
        this.eventBus = eventBus;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        logger.info("channel registered: {}", ctx.channel());
        ctx.channel().attr(consumerAttr).set(new ChannelConsumer(ctx.channel()));
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("channel inactive: {}", ctx.channel());
        ctx.channel().attr(consumerAttr).get().unsubscribe();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("unexpected exception", cause);
        super.exceptionCaught(ctx, cause);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) throws Exception {

        if (frame instanceof TextWebSocketFrame) {

            String json = ((TextWebSocketFrame) frame).text();
            logger.info("received {}", ctx.channel(), json);

            Map<String, String> request = jsonService.deserialize(json, new TypeReference<Map<String, String>>() {
            });
            String action = request.getOrDefault("action", "");
            switch (action) {
                case "subscribe": {
                    String channel = request.getOrDefault("channel", "");
                    ctx.channel().attr(consumerAttr).get().subscribe(channel);
                    break;
                }
                case "unsubscribe": {
                    String channel = request.getOrDefault("channel", "");
                    ctx.channel().attr(consumerAttr).get().unsubscribe(channel);
                    break;
                }
                default:
                    logger.warn("unexpected action");
            }

        } else {
            String message = "unsupported frame type: " + frame.getClass().getName();
            throw new UnsupportedOperationException(message);
        }
    }

    private class ChannelConsumer implements BiConsumer<String, DagRunEvent> {
        private final Channel wsChannel;
        private final CopyOnWriteArraySet<String> channels;

        public ChannelConsumer(Channel wsChannel) {
            this.wsChannel = wsChannel;
            channels = new CopyOnWriteArraySet<>();
        }

        @Override
        public void accept(String channel, DagRunEvent event) {
            String json = jsonService.serialize(ImmutableMap.of("channel", channel, "event", event));
            logger.info("send {}: {}", channel, json);
            wsChannel.writeAndFlush(new TextWebSocketFrame(json));
        }

        public void unsubscribe(String channel) {
            logger.info("unsubscribe {}", channel);
            eventBus.unsubscribe(channel, this);
            channels.remove(channel);
        }

        public void unsubscribe() {
            for (String channel : channels) {
                logger.info("unsubscribe {}", channel);
                eventBus.unsubscribe(channel, this);
            }
            channels.clear();
        }

        public void subscribe(String channel) {
            logger.info("subscribe {}", channel);
            eventBus.subscribe(channel, this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ChannelConsumer that = (ChannelConsumer) o;

            return wsChannel.equals(that.wsChannel);
        }

        @Override
        public int hashCode() {
            return wsChannel.hashCode();
        }
    }
}
