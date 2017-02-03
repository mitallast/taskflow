package org.github.mitallast.taskflow.rest.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.stream.ChunkedFile;
import org.github.mitallast.taskflow.rest.RestResponse;
import org.github.mitallast.taskflow.rest.RestSession;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import static com.google.common.net.HttpHeaders.CONNECTION;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpSession implements RestSession {

    private final FullHttpRequest request;
    private final ChannelHandlerContext ctx;

    public HttpSession(ChannelHandlerContext ctx, FullHttpRequest request) {
        this.request = request;
        this.ctx = ctx;
    }

    @Override
    public ByteBufAllocator alloc() {
        return ctx.alloc();
    }

    @Override
    public void sendResponse(RestResponse response) {
        ByteBuf buffer = response.getBuffer();
        DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(
            HTTP_1_1, response.getResponseStatus(), buffer, false, true);

        int bytes = httpResponse.content().readableBytes();
        if (bytes >= 0) {
            httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes);
        }
        if (HttpUtil.isKeepAlive(request)) {
            httpResponse.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }
        ChannelFuture writeFuture = ctx.writeAndFlush(httpResponse);
        if (HttpUtil.isKeepAlive(request)) {
            writeFuture.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void sendResponse(Throwable response) {
        ByteBuf buffer = ctx.alloc().buffer();
        try (ByteBufOutputStream outputStream = new ByteBufOutputStream(buffer)) {
            try (PrintWriter printWriter = new PrintWriter(outputStream)) {
                response.printStackTrace(printWriter);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(
            HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, buffer, false, true);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, httpResponse.content().readableBytes());
        httpResponse.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        ctx.writeAndFlush(httpResponse).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void sendFile(URL url) throws IOException {
        try {
            sendFile(new File(url.toURI()));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void sendFile(URI uri) throws IOException {
        sendFile(new File(uri));
    }

    @Override
    public void sendFile(File file) throws IOException {
        RandomAccessFile raf;
        raf = new RandomAccessFile(file, "r");
        long fileLength = raf.length();

        DefaultHttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        HttpUtil.setContentLength(response, fileLength);
        setContentTypeHeader(response, file);
        if (HttpUtil.isKeepAlive(request)) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }
        ctx.write(response);

        ChannelFuture write = ctx.writeAndFlush(new HttpChunkedInput(new ChunkedFile(raf, 0, fileLength, 8192)));

        // Decide whether to close the connection or not.
        if (!HttpUtil.isKeepAlive(request)) {
            // Close the connection when the whole content is written out.
            write.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static void setContentTypeHeader(DefaultHttpResponse response, File file) {
        MimetypesFileTypeMap mimeTypesMap = new MimetypesFileTypeMap();
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, mimeTypesMap.getContentType(file.getPath()));
    }
}
