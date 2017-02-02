package org.github.mitallast.taskflow.rest.response;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.rest.netty.HttpResponse;

public class StatusRestResponse extends HttpResponse {
    public StatusRestResponse(HttpResponseStatus status) {
        super(status, Unpooled.EMPTY_BUFFER);
    }
}
