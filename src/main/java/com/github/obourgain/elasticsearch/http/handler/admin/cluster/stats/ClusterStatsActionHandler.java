package com.github.obourgain.elasticsearch.http.handler.admin.cluster.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.admin.HttpClusterAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class ClusterStatsActionHandler implements ActionHandler<ClusterStatsRequest, ClusterStatsResponse, ClusterStatsRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(ClusterStatsActionHandler.class);

    private final HttpClusterAdminClient indicesAdminClient;

    public ClusterStatsActionHandler(HttpClusterAdminClient httpClusterAdminClient) {
        this.indicesAdminClient = httpClusterAdminClient;
    }

    @Override
    public ClusterStatsAction getAction() {
        return ClusterStatsAction.INSTANCE;
    }

    @Override
    public void execute(ClusterStatsRequest request, final ActionListener<ClusterStatsResponse> listener) {
        logger.debug("cluster stats request {}", request);
        try {
            String indices = Strings.arrayToCommaDelimitedString(request.nodesIds());
            if(!indices.isEmpty()) {
                indices = "/nodes/" + indices;
            }

            HttpClientImpl httpClient = indicesAdminClient.getHttpClient();
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(httpClient.getUrl() + "/_cluster/stats" + indices);

            httpRequest.execute(new ListenerAsyncCompletionHandler<ClusterStatsResponse>(listener) {
                        @Override
                        protected ClusterStatsResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toClusterStatsResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
