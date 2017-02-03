package org.github.mitallast.taskflow.rest;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.AsciiString;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;

public interface ResponseBuilder {

    ResponseBuilder status(int status);

    ResponseBuilder status(int status, String reason);

    ResponseBuilder status(HttpResponseStatus status);

    ResponseBuilder header(AsciiString name, AsciiString value);

    void error(Throwable throwable);

    void text(String content);

    void data(ByteBuf content);

    void file(URL url) throws IOException;

    void file(URI uri) throws IOException;

    void file(File file) throws IOException;

    void empty();
}
