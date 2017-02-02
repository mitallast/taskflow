package org.github.mitallast.taskflow.rest.netty;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.github.mitallast.taskflow.common.netty.NettyServer;
import org.github.mitallast.taskflow.rest.RestController;

public class HttpServer extends NettyServer {
    private RestController restController;

    @Inject
    public HttpServer(Config config, RestController restController) {
        super(config.getConfig("rest"), HttpServer.class);
        this.restController = restController;
    }

    @Override
    protected ChannelInitializer<SocketChannel> channelInitializer() {
        return new HttpServerInitializer(new HttpServerHandler(restController));
    }
}
