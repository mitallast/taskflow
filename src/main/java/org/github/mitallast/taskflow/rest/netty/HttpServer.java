package org.github.mitallast.taskflow.rest.netty;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.github.mitallast.taskflow.common.netty.NettyServer;

public class HttpServer extends NettyServer {
    private final HttpServerHandler serverHandler;
    private final WebSocketFrameHandler webSocketFrameHandler;

    @Inject
    public HttpServer(Config config, HttpServerHandler serverHandler, WebSocketFrameHandler webSocketFrameHandler) {
        super(config.getConfig("rest"), HttpServer.class);
        this.serverHandler = serverHandler;
        this.webSocketFrameHandler = webSocketFrameHandler;
    }

    @Override
    protected ChannelInitializer<SocketChannel> channelInitializer() {
        return new HttpServerInitializer(serverHandler, webSocketFrameHandler);
    }
}
