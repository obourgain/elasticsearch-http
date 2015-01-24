package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
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
public class GetAliasesActionHandler implements ActionHandler<GetAliasesRequest, GetAliasesResponse, GetAliasesRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(GetAliasesActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public GetAliasesActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public GetAliasesAction getAction() {
        return GetAliasesAction.INSTANCE;
    }

    @Override
    public void execute(GetAliasesRequest request, final ActionListener<GetAliasesResponse> listener) {
        logger.debug("get aliases request {}", request);
        try {
            String indices = HttpRequestUtils.indicesOrAll(request);
            String aliases = Strings.arrayToCommaDelimitedString(request.aliases());
            if(!aliases.isEmpty()) {
                aliases = "/" + aliases;
            }

            HttpClient httpClient = indicesAdminClient.getHttpClient();
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(httpClient.getUrl() + "/" + indices + "/_alias" + aliases);

            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest.addQueryParam("master_timeout", request.masterNodeTimeout().toString());
            httpRequest.addQueryParam("local", String.valueOf(request.local()));

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<GetAliasesResponse>(listener) {
                        @Override
                        protected GetAliasesResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toGetAliasesResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
