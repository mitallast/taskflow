package org.github.mitallast.taskflow.rest.handler;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.handler.codec.http.HttpMethod;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.rest.RestController;
import org.github.mitallast.taskflow.rest.RestHandler;
import org.github.mitallast.taskflow.rest.RestRequest;

public class IndexHandler extends AbstractComponent implements RestHandler {
    @Inject
    public IndexHandler(Config config, RestController controller) {
        super(config, IndexHandler.class);
        controller.registerHandler(HttpMethod.GET, "/", this);
    }

    @Override
    public void handleRequest(RestRequest request) {
        request.response().text("OK");
    }
}
