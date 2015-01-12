package com.github.obourgain.elasticsearch.http.handler.admin.indices.open;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequestAccessor;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.admin.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

public class OpenIndexActionHandler implements ActionHandler<OpenIndexRequest, OpenIndexResponse, OpenIndexRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(OpenIndexActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public OpenIndexActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public OpenIndexAction getAction() {
        return OpenIndexAction.INSTANCE;
    }

    @Override
    public void execute(OpenIndexRequest request, final ActionListener<OpenIndexResponse> listener) {
        logger.debug("open index request {}", request);
        try {
            String indices = Strings.arrayToCommaDelimitedString(OpenIndexRequestAccessor.indices(request));
            if(!indices.isEmpty()) {
                indices = "/" + indices;
            }

            HttpClientImpl httpClient = indicesAdminClient.getHttpClient();
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(httpClient.getUrl() + indices + "/_open");

            httpRequest.addQueryParam("timeout", String.valueOf(request.timeout()));
            httpRequest.addQueryParam("master_timeout", String.valueOf(request.masterNodeTimeout()));
            HttpRequestUtils.addIndicesOptions(httpRequest, request);

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<OpenIndexResponse>(listener) {
                        @Override
                        protected OpenIndexResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toOpenIndexResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
