package org.github.mitallast.taskflow.rest.action;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.rest.RestController;
import org.github.mitallast.taskflow.rest.RestHandler;
import org.github.mitallast.taskflow.rest.RestRequest;
import org.github.mitallast.taskflow.rest.RestSession;
import org.github.mitallast.taskflow.rest.netty.HttpResponse;
import org.github.mitallast.taskflow.rest.response.StatusRestResponse;

import java.io.IOException;
import java.io.InputStream;

public class ResourceAction extends AbstractComponent implements RestHandler {

    @Inject
    public ResourceAction(Config config, RestController controller) throws IOException {
        super(config.getConfig("rest"), ResourceAction.class);

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
                String resourcePath = resource.getResourceName().substring("org/github/mitallast/taskflow/static".length());
                logger.info("register {}", resourcePath);
                controller.registerHandler(HttpMethod.GET, resourcePath, this);
                controller.registerHandler(HttpMethod.HEAD, resourcePath, this);
            });
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        logger.trace("try find {}", request.getQueryPath());
        final InputStream resourceStream;
        if (request.getQueryPath().startsWith("/resources/webjars/")) {
            resourceStream = getClass().getResourceAsStream("/META-INF" + request.getQueryPath());
        } else if (request.getQueryPath().startsWith("/admin/")) {
            resourceStream = getClass().getResourceAsStream("/org/github/mitallast/taskflow/static" + request.getQueryPath());
        } else {
            resourceStream = null;
        }

        if (resourceStream == null) {
            session.sendResponse(new StatusRestResponse(HttpResponseStatus.NOT_FOUND));
        } else {
            try {
                ByteBuf buffer = session.alloc().buffer();
                byte[] bytes = new byte[1024];
                int read;
                do {
                    read = resourceStream.read(bytes);
                    if (read > 0) {
                        buffer.writeBytes(bytes, 0, read);
                    }
                } while (read > 0);
                session.sendResponse(new HttpResponse(HttpResponseStatus.OK, buffer));
            } catch (IOException e) {
                session.sendResponse(e);
            }
        }
    }
}
