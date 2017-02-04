package org.github.mitallast.taskflow.rest;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.common.component.AbstractComponent;
import org.github.mitallast.taskflow.common.json.JsonService;
import org.github.mitallast.taskflow.common.path.PathTrie;
import org.github.mitallast.taskflow.rest.netty.HttpRequest;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class RestController extends AbstractComponent {

    private final JsonService jsonService;

    private final PathTrie<RestHandler> getHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> postHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> putHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> deleteHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> headHandlers = new PathTrie<>();
    private final PathTrie<RestHandler> optionsHandlers = new PathTrie<>();

    private final ResponseMappers responseMappers;
    private final ParamMappers paramMappers;

    @Inject
    public RestController(Config config, JsonService jsonService) {
        super(config.getConfig("rest"), RestController.class);
        this.jsonService = jsonService;

        this.responseMappers = new ResponseMappers();
        this.paramMappers = new ParamMappers();
    }

    public void dispatchRequest(ChannelHandlerContext ctx, FullHttpRequest httpRequest) {
        final HttpRequest request = new HttpRequest(ctx, httpRequest, jsonService);
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

    public void register(HttpMethod method, String path, RestHandler handler) {
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

    /**
     * Functional API
     */

    public ResponseMappers response() {
        return responseMappers;
    }

    public ParamMappers param() {
        return paramMappers;
    }

    public <R> FunctionMapperBuilder<R> handler(FunctionHandler<R> handler) {
        return new FunctionMapperBuilder<>(handler);
    }

    public <R, P1> Function1MapperBuilder<R, P1> handler(Function1Handler<R, P1> handler) {
        return new Function1MapperBuilder<>(handler);
    }

    public <R, P1, P2> Function2MapperBuilder<R, P1, P2> handler(Function2Handler<R, P1, P2> handler) {
        return new Function2MapperBuilder<>(handler);
    }

    public <R, P1, P2, P3> Function3MapperBuilder<R, P1, P2, P3> handler(Function3Handler<R, P1, P2, P3> handler) {
        return new Function3MapperBuilder<>(handler);
    }

    /**
     * Functional mappers
     */

    public final class ResponseMappers {

        public Consumer<RestRequest> notFound() {
            return request -> request.response().status(HttpResponseStatus.NOT_FOUND).empty();
        }

        public BiConsumer<RestRequest, String> text() {
            return (request, response) -> request.response().text(response);
        }

        public <T> BiConsumer<RestRequest, T> json() {
            return (request, t) -> request.response().json(t);
        }

        public <T> BiConsumer<RestRequest, Optional<T>> optionalJson() {
            return optional(json());
        }

        public BiConsumer<RestRequest, URL> url() {
            return (request, file) -> request.response().file(file);
        }

        public BiConsumer<RestRequest, Optional<URL>> optionalUrl() {
            return optional(url());
        }

        public BiConsumer<RestRequest, URI> uri() {
            return (request, file) -> request.response().file(file);
        }

        public BiConsumer<RestRequest, File> file() {
            return (request, file) -> request.response().file(file);
        }

        public <T> BiConsumer<RestRequest, Optional<T>> optional(BiConsumer<RestRequest, T> mapper) {
            return optional(mapper, notFound());
        }

        public <T> BiConsumer<RestRequest, Optional<T>> optional(BiConsumer<RestRequest, T> mapper, Consumer<RestRequest> empty) {
            return (request, optional) -> {
                if (optional.isPresent()) {
                    mapper.accept(request, optional.get());
                } else {
                    empty.accept(request);
                }
            };
        }
    }

    public final class ParamMappers {
        public Function<RestRequest, RestRequest> request() {
            return request -> request;
        }

        public Function<RestRequest, ByteBuf> content() {
            return RestRequest::content;
        }

        public Function<RestRequest, HttpMethod> method() {
            return RestRequest::getHttpMethod;
        }

        public Function<RestRequest, String> uri() {
            return RestRequest::getUri;
        }

        public Function<RestRequest, String> path() {
            return RestRequest::getQueryPath;
        }

        public Function<RestRequest, String> string(String name) {
            return request -> request.param(name);
        }

        public Function<RestRequest, Integer> toInt(String name) {
            return string(name).andThen(Integer::valueOf);
        }

        public Function<RestRequest, Long> toLong(String name) {
            return string(name).andThen(Long::valueOf);
        }

        public Function<RestRequest, Boolean> toBoolean(String name) {
            return string(name).andThen(Boolean::valueOf);
        }

        public <T> Function<RestRequest, T> json() {
            return request -> jsonService.deserialize(request.content());
        }
    }

    public final class FunctionMapperBuilder<R> {
        private final FunctionHandler<R> handler;
        private BiConsumer<RestRequest, R> responseMapper;

        public FunctionMapperBuilder(FunctionHandler<R> handler) {
            this.handler = handler;
        }

        public FunctionMapperBuilder<R> response(BiConsumer<RestRequest, R> responseMapper) {
            this.responseMapper = responseMapper;
            return this;
        }

        public FunctionMapper<R> build() {
            Preconditions.checkNotNull(handler);
            Preconditions.checkNotNull(responseMapper);
            return new FunctionMapper<R>(handler, responseMapper);
        }

        public void handle(HttpMethod method, String path) {
            register(method, path, build());
        }
    }

    public final class Function1MapperBuilder<R, P1> {
        private final Function1Handler<R, P1> handler;
        private BiConsumer<RestRequest, R> responseMapper;
        private Function<RestRequest, P1> param1mapper;

        public Function1MapperBuilder(Function1Handler<R, P1> handler) {
            this.handler = handler;
        }

        public Function1MapperBuilder<R, P1> response(BiConsumer<RestRequest, R> responseMapper) {
            this.responseMapper = responseMapper;
            return this;
        }

        public Function1MapperBuilder<R, P1> param1(Function<RestRequest, P1> param1mapper) {
            this.param1mapper = param1mapper;
            return this;
        }

        public RestHandler build() {
            Preconditions.checkNotNull(handler);
            Preconditions.checkNotNull(responseMapper);
            Preconditions.checkNotNull(param1mapper);
            return new Function1Mapper<R, P1>(handler, responseMapper, param1mapper);
        }

        public void handle(HttpMethod method, String path) {
            register(method, path, build());
        }
    }

    public final class Function2MapperBuilder<R, P1, P2> {
        private final Function2Handler<R, P1, P2> handler;
        private BiConsumer<RestRequest, R> responseMapper;
        private Function<RestRequest, P1> param1mapper;
        private Function<RestRequest, P2> param2mapper;

        public Function2MapperBuilder(Function2Handler<R, P1, P2> handler) {
            this.handler = handler;
        }

        public Function2MapperBuilder<R, P1, P2> response(BiConsumer<RestRequest, R> responseMapper) {
            this.responseMapper = responseMapper;
            return this;
        }

        public Function2MapperBuilder<R, P1, P2> param1(Function<RestRequest, P1> param1mapper) {
            this.param1mapper = param1mapper;
            return this;
        }

        public Function2MapperBuilder<R, P1, P2> param2(Function<RestRequest, P2> param2mapper) {
            this.param2mapper = param2mapper;
            return this;
        }

        public RestHandler build() {
            Preconditions.checkNotNull(handler);
            Preconditions.checkNotNull(responseMapper);
            Preconditions.checkNotNull(param1mapper);
            Preconditions.checkNotNull(param2mapper);
            return new Function2Mapper<R, P1, P2>(handler, responseMapper, param1mapper, param2mapper);
        }

        public void handle(HttpMethod method, String path) {
            register(method, path, build());
        }
    }

    public final class Function3MapperBuilder<R, P1, P2, P3> {
        private final Function3Handler<R, P1, P2, P3> handler;
        private BiConsumer<RestRequest, R> responseMapper;
        private Function<RestRequest, P1> param1mapper;
        private Function<RestRequest, P2> param2mapper;
        private Function<RestRequest, P3> param3mapper;

        public Function3MapperBuilder(Function3Handler<R, P1, P2, P3> handler) {
            this.handler = handler;
        }

        public Function3MapperBuilder<R, P1, P2, P3> response(BiConsumer<RestRequest, R> responseMapper) {
            this.responseMapper = responseMapper;
            return this;
        }

        public Function3MapperBuilder<R, P1, P2, P3> param1(Function<RestRequest, P1> param1mapper) {
            this.param1mapper = param1mapper;
            return this;
        }

        public Function3MapperBuilder<R, P1, P2, P3> param2(Function<RestRequest, P2> param2mapper) {
            this.param2mapper = param2mapper;
            return this;
        }

        public Function3MapperBuilder<R, P1, P2, P3> param3(Function<RestRequest, P3> param3mapper) {
            this.param3mapper = param3mapper;
            return this;
        }

        public RestHandler build() {
            Preconditions.checkNotNull(handler);
            Preconditions.checkNotNull(responseMapper);
            Preconditions.checkNotNull(param1mapper);
            Preconditions.checkNotNull(param2mapper);
            Preconditions.checkNotNull(param3mapper);
            return new Function3Mapper<R, P1, P2, P3>(handler, responseMapper, param1mapper, param2mapper, param3mapper);
        }

        public void handle(HttpMethod method, String path) {
            register(method, path, build());
        }
    }

    public final class FunctionMapper<R> implements RestHandler {
        private final FunctionHandler<R> handler;
        private final BiConsumer<RestRequest, R> responseMapper;

        public FunctionMapper(FunctionHandler<R> handler, BiConsumer<RestRequest, R> responseMapper) {
            this.handler = handler;
            this.responseMapper = responseMapper;
        }

        @Override
        public void handleRequest(RestRequest request) {
            R response = handler.handleRequest();
            responseMapper.accept(request, response);
        }
    }

    public final class Function1Mapper<R, P1> implements RestHandler {
        private final Function1Handler<R, P1> handler;
        private final BiConsumer<RestRequest, R> responseMapper;
        private final Function<RestRequest, P1> param1mapper;

        public Function1Mapper(Function1Handler<R, P1> handler, BiConsumer<RestRequest, R> responseMapper, Function<RestRequest, P1> param1mapper) {
            this.handler = handler;
            this.responseMapper = responseMapper;
            this.param1mapper = param1mapper;
        }

        @Override
        public void handleRequest(RestRequest request) {
            P1 param1 = param1mapper.apply(request);
            R response = handler.handleRequest(param1);
            responseMapper.accept(request, response);
        }
    }

    public final class Function2Mapper<R, P1, P2> implements RestHandler {
        private final Function2Handler<R, P1, P2> handler;
        private final BiConsumer<RestRequest, R> responseMapper;
        private final Function<RestRequest, P1> param1mapper;
        private final Function<RestRequest, P2> param2mapper;

        public Function2Mapper(Function2Handler<R, P1, P2> handler, BiConsumer<RestRequest, R> responseMapper, Function<RestRequest, P1> param1mapper, Function<RestRequest, P2> param2mapper) {
            this.handler = handler;
            this.responseMapper = responseMapper;
            this.param1mapper = param1mapper;
            this.param2mapper = param2mapper;
        }

        @Override
        public void handleRequest(RestRequest request) {
            P1 param1 = param1mapper.apply(request);
            P2 param2 = param2mapper.apply(request);
            R response = handler.handleRequest(param1, param2);
            responseMapper.accept(request, response);
        }
    }

    public final class Function3Mapper<R, P1, P2, P3> implements RestHandler {
        private final Function3Handler<R, P1, P2, P3> handler;
        private final BiConsumer<RestRequest, R> responseMapper;
        private final Function<RestRequest, P1> param1mapper;
        private final Function<RestRequest, P2> param2mapper;
        private final Function<RestRequest, P3> param3mapper;

        public Function3Mapper(
            Function3Handler<R, P1, P2, P3> handler,
            BiConsumer<RestRequest, R> responseMapper,
            Function<RestRequest, P1> param1mapper,
            Function<RestRequest, P2> param2mapper,
            Function<RestRequest, P3> param3mapper
        ) {
            this.handler = handler;
            this.responseMapper = responseMapper;
            this.param1mapper = param1mapper;
            this.param2mapper = param2mapper;
            this.param3mapper = param3mapper;
        }

        @Override
        public void handleRequest(RestRequest request) {
            P1 param1 = param1mapper.apply(request);
            P2 param2 = param2mapper.apply(request);
            P3 param3 = param3mapper.apply(request);
            R response = handler.handleRequest(param1, param2, param3);
            responseMapper.accept(request, response);
        }
    }

    public interface FunctionHandler<R> {
        R handleRequest();
    }

    public interface Function1Handler<R, P1> {
        R handleRequest(P1 p1);
    }

    public interface Function2Handler<R, P1, P2> {
        R handleRequest(P1 p1, P2 p2);
    }

    public interface Function3Handler<R, P1, P2, P3> {
        R handleRequest(P1 p1, P2 p2, P3 p3);
    }
}
