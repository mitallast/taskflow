package org.github.mitallast.taskflow.rest.netty;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.handler.stream.ChunkedFile;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;
import org.github.mitallast.taskflow.common.json.JsonService;
import org.github.mitallast.taskflow.rest.ResponseBuilder;
import org.github.mitallast.taskflow.rest.RestRequest;

import javax.activation.MimetypesFileTypeMap;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpRequest implements RestRequest {

    private final ChannelHandlerContext ctx;
    private final FullHttpRequest httpRequest;
    private final HttpMethod httpMethod;
    private final JsonService jsonService;

    private Map<String, String> paramMap;
    private String queryPath;

    public HttpRequest(ChannelHandlerContext ctx, FullHttpRequest request, JsonService jsonService) {
        this.ctx = ctx;
        this.httpRequest = request;
        this.httpMethod = request.method();
        this.jsonService = jsonService;

        parseQueryString();
    }

    @Override
    public HttpMethod getHttpMethod() {
        return httpMethod;
    }

    @Override
    public Map<String, String> getParamMap() {
        return paramMap;
    }

    @Override
    public String param(String param) {
        String val = paramMap.get(param);
        if (val == null) {
            throw new IllegalArgumentException("Param {" + param + "} not found");
        } else {
            return val;
        }
    }

    @Override
    public boolean hasParam(String param) {
        return paramMap.containsKey(param);
    }

    @Override
    public ByteBuf content() {
        return httpRequest.content();
    }

    @Override
    public String getQueryPath() {
        return queryPath;
    }

    @Override
    public String getUri() {
        return httpRequest.uri();
    }

    private void parseQueryString() {
        String uri = httpRequest.uri();

        int pathEndPos = uri.indexOf('?');

        if (pathEndPos < 0) {
            paramMap = new HashMap<>();
            queryPath = uri;
        } else {
            QueryStringDecoder decoder = new QueryStringDecoder(uri);
            queryPath = uri.substring(0, pathEndPos);
            Map<String, List<String>> parameters = decoder.parameters();
            if (parameters.isEmpty()) {
                paramMap = Collections.emptyMap();
            } else {
                paramMap = new HashMap<>(parameters.size());
                parameters.entrySet().stream()
                    .filter(entry -> entry.getValue() != null && !entry.getValue().isEmpty())
                    .forEach(entry -> paramMap.put(entry.getKey(), entry.getValue().get(0)));
            }
        }
    }

    @Override
    public ResponseBuilder response() {
        return new HttpResponseBuilder();
    }

    private class HttpResponseBuilder implements ResponseBuilder {
        private HttpResponseStatus status = HttpResponseStatus.OK;
        private final HttpHeaders headers = new DefaultHttpHeaders(false);

        @Override
        public ResponseBuilder status(int status) {
            this.status = HttpResponseStatus.valueOf(status);
            return this;
        }

        @Override
        public ResponseBuilder status(int status, String reason) {
            Preconditions.checkNotNull(reason);
            this.status = new HttpResponseStatus(status, reason);
            return this;
        }

        @Override
        public ResponseBuilder status(HttpResponseStatus status) {
            Preconditions.checkNotNull(status);
            this.status = status;
            return this;
        }

        @Override
        public ResponseBuilder header(AsciiString name, AsciiString value) {
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(value);
            headers.add(name, value);
            return this;
        }

        @Override
        public void error(Throwable error) {
            Preconditions.checkNotNull(error);
            ByteBuf buffer = ctx.alloc().buffer();
            try (ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer)) {
                try (PrintWriter printWriter = new PrintWriter(outputStream)) {
                    error.printStackTrace(printWriter);
                }
            } catch (Exception e) {
                buffer.release();
                throw new RuntimeException(e);
            }
            header(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
            data(buffer);
        }

        @Override
        public void json(Object json) {
            Preconditions.checkNotNull(json);
            ByteBuf buf = ctx.alloc().buffer();
            jsonService.serialize(buf, json);
            header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
            data(buf);
        }

        @Override
        public void text(String content) {
            Preconditions.checkNotNull(content);
            header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
            data(Unpooled.copiedBuffer(content, CharsetUtil.UTF_8));
        }

        @Override
        public void data(ByteBuf content) {
            Preconditions.checkNotNull(content);
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status,
                content,
                headers,
                EmptyHttpHeaders.INSTANCE
            );
            HttpUtil.setContentLength(response, content.readableBytes());
            if (HttpUtil.isKeepAlive(httpRequest)) {
                HttpUtil.setKeepAlive(response, true);
            }
            ChannelFuture writeFuture = ctx.writeAndFlush(response);
            if (!HttpUtil.isKeepAlive(httpRequest)) {
                writeFuture.addListener(ChannelFutureListener.CLOSE);
            }
        }

        @Override
        public void file(URL url) {
            try {
                file(url.toURI());
            } catch (URISyntaxException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void file(URI uri) {
            file(new File(uri));
        }

        @Override
        public void file(File file) {
            RandomAccessFile raf;
            try {
                raf = new RandomAccessFile(file, "r");
            } catch (FileNotFoundException e) {
                throw new IOError(e);
            }
            long fileLength = 0;
            try {
                fileLength = raf.length();
            } catch (IOException e) {
                try {
                    raf.close();
                } catch (IOException ignore) {
                }
                throw new IOError(e);
            }

            DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK, headers);
            HttpUtil.setContentLength(response, fileLength);

            MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, mimeTypesMap.getContentType(file.getPath()));

            if (HttpUtil.isKeepAlive(httpRequest)) {
                HttpUtil.setKeepAlive(response, true);
            }
            ctx.write(response);
            ChannelFuture write = null;
            try {
                write = ctx.writeAndFlush(new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength, 8192)));
                if (!HttpUtil.isKeepAlive(httpRequest)) {
                    write.addListener(ChannelFutureListener.CLOSE);
                }
            } catch (IOException e) {
                try {
                    raf.close();
                } catch (IOException ignore) {
                }
                throw new IOError(e);
            }
        }

        @Override
        public void empty() {
            DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1,
                status,
                Unpooled.buffer(0),
                headers,
                EmptyHttpHeaders.INSTANCE
            );
            if (HttpUtil.isKeepAlive(httpRequest)) {
                HttpUtil.setKeepAlive(response, true);
            }
            ChannelFuture write = ctx.writeAndFlush(response);
            if (!HttpUtil.isKeepAlive(httpRequest)) {
                write.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }
}
