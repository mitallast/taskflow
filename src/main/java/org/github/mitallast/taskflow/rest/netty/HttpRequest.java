package org.github.mitallast.taskflow.rest.netty;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.github.mitallast.taskflow.rest.RestRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpRequest implements RestRequest {

    public static final String METHOD_TUNNEL = "_method";
    private FullHttpRequest httpRequest;
    private HttpMethod httpMethod;
    private HttpHeaders httpHeaders;

    private Map<String, String> paramMap;
    private String queryPath;

    public HttpRequest(FullHttpRequest request) {
        this.httpRequest = request;
        this.httpMethod = request.method();
        this.httpHeaders = request.headers();
        this.parseQueryString();
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
        return paramMap.get(param);
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
}
