package com.github.obourgain.elasticsearch.http.handler.admin.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.client.HttpClusterAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class ClusterHealthActionHandler implements ActionHandler<ClusterHealthRequest, ClusterHealthResponse, ClusterHealthRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(ClusterHealthActionHandler.class);

    private final HttpClusterAdminClient indicesAdminClient;

    public ClusterHealthActionHandler(HttpClusterAdminClient httpClusterAdminClient) {
        this.indicesAdminClient = httpClusterAdminClient;
    }

    @Override
    public ClusterHealthAction getAction() {
        return ClusterHealthAction.INSTANCE;
    }

    @Override
    public void execute(ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
        logger.debug("cluster health request {}", request);
        try {
            HttpClient httpClient = indicesAdminClient.getHttpClient();
            StringBuilder url = new StringBuilder();
            url.append(httpClient.getUrl());
            url.append("/_cluster/health");
            if (request.indices().length != 0) {
                url.append("/").append(Strings.arrayToCommaDelimitedString(request.indices()));
            }

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(url.toString());

            // TODO level ? http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/cluster-health.html#request-params
            httpRequest.addQueryParam("level", "shards");

            if (request.waitForStatus() != null) {
                httpRequest.addQueryParam("wait_for_status", request.waitForStatus().name().toLowerCase());
            }
            if (request.waitForRelocatingShards() != -1) {
                httpRequest.addQueryParam("wait_for_relocating_shards", String.valueOf(request.waitForRelocatingShards()));
            }
            if (!request.waitForNodes().equals("")) {
                httpRequest.addQueryParam("wait_for_nodes", request.waitForNodes());
            }
            httpRequest.addQueryParam("timeout", request.timeout().toString());
            httpRequest.addQueryParam("master_timeout", request.masterNodeTimeout().toString());

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<ClusterHealthResponse>(request, listener) {
                        @Override
                        protected ClusterHealthResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toClusterHealthResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
