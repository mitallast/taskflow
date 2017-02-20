package org.github.mitallast.taskflow.rest;

import com.google.inject.AbstractModule;
import org.github.mitallast.taskflow.rest.handler.*;
import org.github.mitallast.taskflow.rest.netty.HttpServer;
import org.github.mitallast.taskflow.rest.netty.HttpServerHandler;

public class RestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(HttpServer.class).asEagerSingleton();
        bind(HttpServerHandler.class).asEagerSingleton();
        bind(RestController.class).asEagerSingleton();

        bind(ResourceHandler.class).asEagerSingleton();
        bind(DagController.class).asEagerSingleton();
        bind(DagRunController.class).asEagerSingleton();
        bind(DagScheduleController.class).asEagerSingleton();
        bind(OperationController.class).asEagerSingleton();
    }
}
