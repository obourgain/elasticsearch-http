package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestAccessor;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
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

/**
 * @author olivier bourgain
 */
public class DeleteIndexActionHandler implements ActionHandler<DeleteIndexRequest, DeleteIndexResponse, DeleteIndexRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(DeleteIndexActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public DeleteIndexActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public DeleteIndexAction getAction() {
        return DeleteIndexAction.INSTANCE;
    }

    @Override
    public void execute(DeleteIndexRequest request, final ActionListener<DeleteIndexResponse> listener) {
        logger.debug("delete index request {}", request);
        try {
            // TODO test


            String[] indices = DeleteIndexRequestAccessor.indices(request);
            if(indices.length == 0) {
                // TODO check how the transport client handles this
                throw new IllegalArgumentException("missing indices");
            }

            HttpClientImpl httpClient = indicesAdminClient.getHttpClient();
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareDelete(httpClient.getUrl() + "/" + Strings.arrayToCommaDelimitedString(indices));

            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest.execute(new ListenerAsyncCompletionHandler<DeleteIndexResponse>(listener) {
                @Override
                protected DeleteIndexResponse convert(ResponseWrapper responseWrapper) {
                    return responseWrapper.toDeleteIndexResponse();
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
