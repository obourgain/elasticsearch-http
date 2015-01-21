package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.common.hppc.IntSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClient;
import com.github.obourgain.elasticsearch.http.admin.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.ValidStatusCodes;
import com.github.obourgain.elasticsearch.http.response.admin.indices.flush.FlushResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class FlushActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(FlushActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public FlushActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public FlushAction getAction() {
        return FlushAction.INSTANCE;
    }

    public void execute(FlushRequest request, final ActionListener<FlushResponse> listener) {
        logger.debug("flush request {}", request);
        try {
            HttpClient httpClient = indicesAdminClient.getHttpClient();

            String indices = HttpRequestUtils.indicesOrAll(request);

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(httpClient.getUrl() + "/" + indices + "/_flush");

            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest.addQueryParam("force", String.valueOf(request.force()));
            httpRequest.addQueryParam("full", String.valueOf(request.full()));
            httpRequest.addQueryParam("wait_if_ongoing", String.valueOf(request.waitIfOngoing()));
            // TODO 404
//                {
//                    "error": "IndexMissingException[[twitter22] missing]",
//                        "status": 404
//                }
            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<FlushResponse>(listener) {
                        @Override
                        protected FlushResponse convert(Response response) {
                            return FlushResponse.parse(response);
                        }

                        @Override
                        protected IntSet non200ValidStatuses() {
                            return ValidStatusCodes._404;
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
