package com.github.obourgain.elasticsearch.http.handler.admin.indices.mapping.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClient;
import com.github.obourgain.elasticsearch.http.admin.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class PutMappingActionHandler implements ActionHandler<PutMappingRequest, PutMappingResponse, PutMappingRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(PutMappingActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public PutMappingActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public PutMappingAction getAction() {
        return PutMappingAction.INSTANCE;
    }

    @Override
    public void execute(PutMappingRequest request, final ActionListener<PutMappingResponse> listener) {
        logger.debug("put mapping request {}", request);
        try {
            HttpClient httpClient = indicesAdminClient.getHttpClient();

            String indices = HttpRequestUtils.indicesOrAll(request);

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(httpClient.getUrl() + "/" + indices + "/" + request.type() + "/_mapping");

            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest.addQueryParam("ignore_conflicts", String.valueOf(request.ignoreConflicts()));
            httpRequest.addQueryParam("timeout", request.timeout().toString());
            httpRequest.addQueryParam("master_timeout", request.masterNodeTimeout().toString());

            httpRequest.setBody(request.source());

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<PutMappingResponse>(listener) {
                        @Override
                        protected PutMappingResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toPutMappingResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
