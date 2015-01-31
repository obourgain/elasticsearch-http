package com.github.obourgain.elasticsearch.http.client;

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

    public HttpClusterAdminClient cluster() {
        return clusterAdminClient;
    }

    public HttpIndicesAdminClient indices() {
        return indicesAdminClient;
    }
}
