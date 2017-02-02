package org.github.mitallast.taskflow.rest.response;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.github.mitallast.taskflow.common.strings.Strings;
import org.github.mitallast.taskflow.rest.netty.HttpResponse;

public class StringRestResponse extends HttpResponse {

    public StringRestResponse(HttpResponseStatus status, String message) {
        super(status, Strings.toByteBuf(message));
    }
}
