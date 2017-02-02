package org.github.mitallast.taskflow.rest.response;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.rest.netty.HttpResponse;

public class ByteBufRestResponse extends HttpResponse {
    public ByteBufRestResponse(HttpResponseStatus status, ByteBuf buffer) {
        super(status, buffer);
    }
}
