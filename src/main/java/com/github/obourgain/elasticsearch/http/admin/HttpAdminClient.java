package com.github.obourgain.elasticsearch.http.admin;

import org.elasticsearch.client.ClusterAdminClient;
import com.github.obourgain.elasticsearch.http.HttpClient;

/**
 * @author olivier bourgain
 */
public class HttpAdminClient {

    private HttpClusterAdminClient clusterAdminClient;
    private HttpIndicesAdminClient indicesAdminClient;

    public HttpAdminClient(HttpClient httpClient) {
        this.clusterAdminClient = new HttpClusterAdminClient(httpClient);
        this.indicesAdminClient = new HttpIndicesAdminClient(httpClient);
    }

    public ClusterAdminClient cluster() {
        return clusterAdminClient;
    }

    public HttpIndicesAdminClient indices() {
        return indicesAdminClient;
    }
}
