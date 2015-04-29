package com.github.obourgain.elasticsearch.http.client;

import com.google.common.base.Supplier;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.*;
import io.reactivex.netty.protocol.http.client.HttpClient;

/**
 * @author olivier bourgain
 */
public class HttpAdminClient {

    private HttpClusterAdminClient clusterAdminClient;
    private HttpIndicesAdminClient indicesAdminClient;

    public HttpAdminClient(Supplier<HttpClient<ByteBuf, ByteBuf>> httpClient) {
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
