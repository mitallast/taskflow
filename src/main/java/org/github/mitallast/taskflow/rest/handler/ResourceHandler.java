package org.github.mitallast.taskflow.rest.handler;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.rest.RestController;
import org.github.mitallast.taskflow.rest.RestHandler;
import org.github.mitallast.taskflow.rest.RestRequest;

import java.io.IOException;
import java.net.URL;

public class ResourceHandler extends AbstractComponent implements RestHandler {

    @Inject
    public ResourceHandler(Config config, RestController controller) throws IOException {
        super(config.getConfig("rest"), ResourceHandler.class);

        ClassPath classPath = ClassPath.from(getClass().getClassLoader());
        ImmutableSet<ClassPath.ResourceInfo> resources = classPath.getResources();
        resources.stream()
            .filter(resource -> resource.getResourceName().startsWith("META-INF/resources/webjars/"))
            .forEach(resource -> {
                String resourcePath = resource.getResourceName().substring("META-INF".length());
                logger.trace("register {}", resourcePath);
                controller.registerHandler(HttpMethod.GET, resourcePath, this);
                controller.registerHandler(HttpMethod.HEAD, resourcePath, this);
            });

        resources.stream()
            .filter(resource -> resource.getResourceName().startsWith("org/github/mitallast/taskflow/static"))
            .forEach(resource -> {
                String resourcePath = resource.getResourceName().substring("org/github/mitallast/taskflow/".length());
                logger.info("register {}", resourcePath);
                controller.registerHandler(HttpMethod.GET, resourcePath, this);
                controller.registerHandler(HttpMethod.HEAD, resourcePath, this);
            });
    }

    @Override
    public void handleRequest(RestRequest request) {
        logger.trace("try find {}", request.getQueryPath());
        URL url;
        if (request.getQueryPath().startsWith("/resources/webjars/")) {
            url = getClass().getResource("/META-INF" + request.getQueryPath());
        } else if (request.getQueryPath().startsWith("/static/")) {
            url = getClass().getResource("/org/github/mitallast/taskflow/" + request.getQueryPath());
        } else {
            url = null;
        }

        if (url == null) {
            logger.trace("not found");
            request.response().status(HttpResponseStatus.NOT_FOUND).empty();
        } else {
            try {
                logger.trace("send {}", url);
                request.response().file(url);
            } catch (IOException e) {
                logger.error(e);
                request.response().status(HttpResponseStatus.INTERNAL_SERVER_ERROR).error(e);
            }
        }
    }
}
