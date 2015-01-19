package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.admin.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.admin.indices.refresh.RefreshResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class RefreshActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(RefreshActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public RefreshActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public RefreshAction getAction() {
        return RefreshAction.INSTANCE;
    }

    public void execute(RefreshRequest request, final ActionListener<RefreshResponse> listener) {
        logger.debug("refresh request {}", request);
        try {
            HttpClientImpl httpClient = indicesAdminClient.getHttpClient();

            String indices = HttpRequestUtils.indicesOrAll(request);

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(httpClient.getUrl() + "/" + indices + "/_refresh");

            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest.addQueryParam("force", String.valueOf(request.force()));
            httpRequest.execute(new ListenerAsyncCompletionHandler<RefreshResponse>(listener) {
                // TODO 404
//                {
//                    "error": "IndexMissingException[[twitter22] missing]",
//                        "status": 404
//                }
                        @Override
                        protected RefreshResponse convert(Response response) {
                            return RefreshResponse.parse(response);
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
