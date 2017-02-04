package org.github.mitallast.taskflow.rest.handler;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.handler.codec.http.HttpMethod;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.rest.RestController;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

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
                controller.handler(this::webjars)
                    .param1(controller.param().path())
                    .response(controller.response().url())
                    .handle(HttpMethod.GET, resourcePath);
            });

        if (false) {
            resources.stream()
                .filter(resource -> resource.getResourceName().startsWith("org/github/mitallast/taskflow/static"))
                .forEach(resource -> {
                    String resourcePath = resource.getResourceName().substring("org/github/mitallast/taskflow/".length());
                    logger.info("register {}", resourcePath);
                    controller.handler(this::resourceStatic)
                        .param1(controller.param().path())
                        .response(controller.response().url())
                        .handle(HttpMethod.GET, resourcePath);
                });
        } else {
            Path root = new File("./src/main/resources/org/github/mitallast/taskflow/static").toPath();
            Files.walk(root)
                .filter(path -> path.toFile().isFile())
                .forEach(path -> {
                    StringBuilder builder = new StringBuilder();
                    for (Path part : root.getParent().relativize(path)) {
                        if (builder.length() > 0) {
                            builder.append('/');
                        }
                        builder.append(part.getFileName());
                    }
                    String resourcePath = builder.toString();

                    logger.info("register {}", resourcePath);
                    controller.handler(this::fileStatic)
                        .param1(controller.param().path())
                        .response(controller.response().file())
                        .handle(HttpMethod.GET, resourcePath);
                });
        }

        controller.handler(this::favicon)
            .response(controller.response().url())
            .handle(HttpMethod.GET, "favicon.ico");
    }

    public URL webjars(String path) {
        return getClass().getResource("/META-INF" + path);
    }

    public URL resourceStatic(String path) {
        return getClass().getResource("/org/github/mitallast/taskflow/" + path);
    }

    public File fileStatic(String path) {
        return new File("src/main/resources/org/github/mitallast/taskflow", path);
    }

    public URL favicon() {
        return getClass().getResource("/favicon.ico");
    }
}
