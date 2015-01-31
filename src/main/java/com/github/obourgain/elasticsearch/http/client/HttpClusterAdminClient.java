package com.github.obourgain.elasticsearch.http.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.ClusterHealthActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.ClusterStateActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.node.hotthreads.NodesHotThreadsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.settings.ClusterUpdateSettingsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.stats.ClusterStatsActionHandler;

/**
 * @author olivier bourgain
 */
public class HttpClusterAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpClusterAdminClient.class);

    private final HttpClient httpClient;

    private ClusterStateActionHandler clusterStateActionHandler = new ClusterStateActionHandler(this);
    private ClusterStatsActionHandler clusterStatsActionHandler = new ClusterStatsActionHandler(this);
    private ClusterHealthActionHandler clusterHealthActionHandler = new ClusterHealthActionHandler(this);
    private ClusterUpdateSettingsActionHandler clusterUpdateSettingsActionHandler = new ClusterUpdateSettingsActionHandler(this);
    private NodesHotThreadsActionHandler nodesHotThreadsActionHandler = new NodesHotThreadsActionHandler(this);

    public HttpClusterAdminClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }
}
