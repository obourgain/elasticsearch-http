package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class GetMappingsActionHandler implements ActionHandler<GetMappingsRequest, GetMappingsResponse, GetMappingsRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(GetMappingsActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public GetMappingsActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public GetMappingsAction getAction() {
        return GetMappingsAction.INSTANCE;
    }

    @Override
    public void execute(GetMappingsRequest request, final ActionListener<GetMappingsResponse> listener) {
        // TODO tests
        logger.debug("get mappings request {}", request);
        try {
            HttpClient httpClient = indicesAdminClient.getHttpClient();

            String indices = HttpRequestUtils.indicesOrAll(request);
            String types = Strings.arrayToCommaDelimitedString(request.types());
            if (!types.isEmpty()) {
                types = "/" + types;
            }

            // lots of url patterns are accepted, but this one is the most practical for a generic impl
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(httpClient.getUrl() + "/" + indices + "/_mapping" + types);

            httpRequest.addQueryParam("master_timeout", request.masterNodeTimeout().toString());

            httpRequest.execute(new ListenerAsyncCompletionHandler<GetMappingsResponse>(listener) {
                @Override
                protected GetMappingsResponse convert(ResponseWrapper responseWrapper) {
                    return responseWrapper.toGetMappingsResponse();
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
