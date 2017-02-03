package org.github.mitallast.taskflow.rest;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;

public interface RestSession {

    ByteBufAllocator alloc();

    void sendResponse(HttpResponseStatus status);

    void sendResponse(HttpResponseStatus status, String response);

    void sendResponse(RestResponse response);

    void sendResponse(Throwable response);

    void sendFile(URL url) throws IOException;

    void sendFile(URI uri) throws IOException;

    void sendFile(File file) throws IOException;
}
