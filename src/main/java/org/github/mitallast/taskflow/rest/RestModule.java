package org.github.mitallast.taskflow.rest;

import com.google.inject.AbstractModule;
import org.github.mitallast.taskflow.rest.netty.HttpServer;

public class RestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(HttpServer.class).asEagerSingleton();
        bind(RestController.class).asEagerSingleton();
    }
}
