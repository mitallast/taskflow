package org.github.mitallast.taskflow.rest;

import com.google.inject.AbstractModule;
import org.github.mitallast.taskflow.operation.Operation;
import org.github.mitallast.taskflow.rest.handler.DagController;
import org.github.mitallast.taskflow.rest.handler.DagRunController;
import org.github.mitallast.taskflow.rest.handler.OperationController;
import org.github.mitallast.taskflow.rest.handler.ResourceHandler;
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
        bind(OperationController.class).asEagerSingleton();
    }
}
