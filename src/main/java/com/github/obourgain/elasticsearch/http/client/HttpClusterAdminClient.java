package com.github.obourgain.elasticsearch.http.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.ClusterHealthActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.ClusterStateActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.node.hotthreads.NodesHotThreadsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.settings.ClusterUpdateSettingsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.stats.ClusterStatsActionHandler;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.*;

/**
 * @author olivier bourgain
 */
public class HttpClusterAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpClusterAdminClient.class);

    private final io.reactivex.netty.protocol.http.client.HttpClient<ByteBuf, ByteBuf> httpClient;

    private ClusterStateActionHandler clusterStateActionHandler = new ClusterStateActionHandler(this);
    private ClusterStatsActionHandler clusterStatsActionHandler = new ClusterStatsActionHandler(this);
    private ClusterHealthActionHandler clusterHealthActionHandler = new ClusterHealthActionHandler(this);
    private ClusterUpdateSettingsActionHandler clusterUpdateSettingsActionHandler = new ClusterUpdateSettingsActionHandler(this);
    private NodesHotThreadsActionHandler nodesHotThreadsActionHandler = new NodesHotThreadsActionHandler(this);

    public HttpClusterAdminClient(io.reactivex.netty.protocol.http.client.HttpClient<ByteBuf, ByteBuf> httpClient) {
        this.httpClient = httpClient;
    }

    public io.reactivex.netty.protocol.http.client.HttpClient<ByteBuf, ByteBuf> getHttpClient() {
        return httpClient;
    }
}
