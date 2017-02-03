package org.github.mitallast.taskflow.rest;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.common.path.PathTrie;
import org.github.mitallast.taskflow.rest.netty.HttpRequest;

public class RestController extends AbstractComponent {

    private final PathTrie<RestHandler> getHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> postHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> putHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> deleteHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> headHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> optionsHandlers = new PathTrie<>();

    @Inject
    public RestController(Config config) {
        super(config.getConfig("rest"), RestController.class);
    }

    public void dispatchRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        final HttpRequest request = new HttpRequest(ctx, httpRequest);
        try {
            executeHandler(request);
        } catch (Throwable e) {
            try {
                request.response()
                    .status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                    .error(e);
            } catch (Throwable ex) {
                logger.error("error send", e);
                logger.error("Failed to send failure response for uri [" + httpRequest.uri() + "]", ex);
            }
        } finally {
            httpRequest.release();
        }
    }

    private void executeHandler(RestRequest request) {
        final RestHandler handler = getHandler(request);
        if (handler != null) {
            handler.handleRequest(request);
        } else {
            if (request.getHttpMethod() == HttpMethod.OPTIONS) {
                request.response()
                    .status(HttpResponseStatus.OK)
                    .empty();
            } else {
                request.response()
                    .status(HttpResponseStatus.BAD_REQUEST)
                    .text("No handler found for uri [" + request.getUri() + "] and method [" + request.getHttpMethod() + "]");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private RestHandler getHandler(RestRequest request) {
        String path = request.getQueryPath();
        HttpMethod method = request.getHttpMethod();
        if (method == HttpMethod.GET) {
            return getHandlers.retrieve(path, request.getParamMap());
        } else if (method == HttpMethod.POST) {
            return postHandlers.retrieve(path, request.getParamMap());
        } else if (method == HttpMethod.PUT) {
            return putHandlers.retrieve(path, request.getParamMap());
        } else if (method == HttpMethod.DELETE) {
            return deleteHandlers.retrieve(path, request.getParamMap());
        } else if (method == HttpMethod.HEAD) {
            return headHandlers.retrieve(path, request.getParamMap());
        } else if (method == HttpMethod.OPTIONS) {
            return optionsHandlers.retrieve(path, request.getParamMap());
        } else {
            return null;
        }
    }

    public void registerHandler(HttpMethod method, String path, RestHandler handler) {
        if (HttpMethod.GET == method) {
            getHandlers.insert(path, handler);
        } else if (HttpMethod.DELETE == method) {
            deleteHandlers.insert(path, handler);
        } else if (HttpMethod.POST == method) {
            postHandlers.insert(path, handler);
        } else if (HttpMethod.PUT == method) {
            putHandlers.insert(path, handler);
        } else if (HttpMethod.OPTIONS == method) {
            optionsHandlers.insert(path, handler);
        } else if (HttpMethod.HEAD == method) {
            headHandlers.insert(path, handler);
        } else {
            throw new IllegalArgumentException("Can't handle [" + method + "] for path [" + path + "]");
        }
    }
}
