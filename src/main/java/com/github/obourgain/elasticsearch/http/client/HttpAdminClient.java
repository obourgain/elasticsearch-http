package com.github.obourgain.elasticsearch.http.client;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.*;

/**
 * @author olivier bourgain
 */
public class HttpAdminClient {

    private HttpClusterAdminClient clusterAdminClient;
    private HttpIndicesAdminClient indicesAdminClient;

    public HttpAdminClient(io.reactivex.netty.protocol.http.client.HttpClient<ByteBuf, ByteBuf> httpClient) {
        this.clusterAdminClient = new HttpClusterAdminClient(httpClient);
        this.indicesAdminClient = new HttpIndicesAdminClient(httpClient);
    }

    public HttpClusterAdminClient cluster() {
        return clusterAdminClient;
    }

    public HttpIndicesAdminClient indices() {
        return indicesAdminClient;
    }
}
