package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.admin.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class FlushActionHandler implements ActionHandler<FlushRequest, FlushResponse, FlushRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(FlushActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public FlushActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public FlushAction getAction() {
        return FlushAction.INSTANCE;
    }

    @Override
    public void execute(FlushRequest request, final ActionListener<FlushResponse> listener) {
        logger.debug("flush request {}", request);
        try {
            HttpClientImpl httpClient = indicesAdminClient.getHttpClient();


            // TODO wait if ongoing as param in more recent versions

            String indices = HttpRequestUtils.indicesOrAll(request);

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(httpClient.getUrl() + "/" + indices + "/_flush");

            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest.addQueryParam("force", String.valueOf(request.force()));
            httpRequest.addQueryParam("full", String.valueOf(request.full()));

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<FlushResponse>(listener) {
                        @Override
                        protected FlushResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toFlushResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
