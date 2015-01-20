package com.github.obourgain.elasticsearch.http.handler.admin.cluster;

import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClient;
import com.github.obourgain.elasticsearch.http.admin.HttpClusterAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.google.common.base.Joiner;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class ClusterStateActionHandler implements ActionHandler<ClusterStateRequest, ClusterStateResponse, ClusterStateRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(ClusterStateActionHandler.class);

    private final HttpClusterAdminClient indicesAdminClient;

    public ClusterStateActionHandler(HttpClusterAdminClient httpClusterAdminClient) {
        this.indicesAdminClient = httpClusterAdminClient;
    }

    @Override
    public ClusterStateAction getAction() {
        return ClusterStateAction.INSTANCE;
    }

    @Override
    public void execute(ClusterStateRequest request, final ActionListener<ClusterStateResponse> listener) {
        logger.debug("cluster state request {}", request);
        try {

            List<String> metricsAsList = new ArrayList<>();
            if(request.blocks()) {
                metricsAsList.add("blocks");
            }
            if(request.routingTable()) {
                metricsAsList.add("routing_table");
            }
            if(request.nodes()) {
                metricsAsList.add("nodes");
            }
            if(request.metaData()) {
                metricsAsList.add("metadata");
            }

            String metrics;
            if(metricsAsList.isEmpty()) {
                metrics = "_all";
            } else {
                // TODO version and master_node are not in the request, so add them by default
                metricsAsList.add("version");
                metricsAsList.add("master_node");
                metrics = Joiner.on(",").join(metricsAsList);
            }

            String indices = Strings.arrayToCommaDelimitedString(request.indices());
            HttpClient httpClient = indicesAdminClient.getHttpClient();
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(httpClient.getUrl() + "/_cluster/state/" + metrics + (indices != null ? "/" + indices : ""));

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<ClusterStateResponse>(listener) {
                        @Override
                        protected ClusterStateResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toClusterStateResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
