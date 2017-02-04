package org.github.mitallast.taskflow.rest.handler;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.handler.codec.http.HttpMethod;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.rest.RestController;

import java.io.IOException;
import java.net.URL;
import java.util.Optional;

public class ResourceHandler extends AbstractComponent {

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
                controller.handler(this::resource)
                    .param1(controller.param().path())
                    .response(controller.response().optionalUrl())
                    .handle(HttpMethod.GET, resourcePath);
            });

        resources.stream()
            .filter(resource -> resource.getResourceName().startsWith("org/github/mitallast/taskflow/static"))
            .forEach(resource -> {
                String resourcePath = resource.getResourceName().substring("org/github/mitallast/taskflow/".length());
                logger.info("register {}", resourcePath);
                controller.handler(this::resource)
                    .param1(controller.param().path())
                    .response(controller.response().optionalUrl())
                    .handle(HttpMethod.GET, resourcePath);
            });
    }

    public Optional<URL> resource(String path) {
        logger.trace("try find {}", path);
        URL url;
        if (path.startsWith("/resources/webjars/")) {
            url = getClass().getResource("/META-INF" + path);
        } else if (path.startsWith("/static/")) {
            url = getClass().getResource("/org/github/mitallast/taskflow/" + path);
        } else {
            url = null;
        }

        return Optional.ofNullable(url);
    }
}
