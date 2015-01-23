package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.Strings;
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
public class GetSettingsActionHandler implements ActionHandler<GetSettingsRequest, GetSettingsResponse, GetSettingsRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(GetSettingsActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public GetSettingsActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public GetSettingsAction getAction() {
        return GetSettingsAction.INSTANCE;
    }

    @Override
    public void execute(GetSettingsRequest request, final ActionListener<GetSettingsResponse> listener) {
        // TODO tests
        logger.debug("get settings request {}", request);
        try {
            HttpClient httpClient = indicesAdminClient.getHttpClient();

            String indices = HttpRequestUtils.indicesOrAll(request);
            String names = Strings.arrayToCommaDelimitedString(request.names());

            if(!names.isEmpty()) {
                names = "/" + names;
            }

            // lots of url patterns are accepted, but this one is the most practical for a generic impl
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(httpClient.getUrl() + "/" + indices + "/_settings" + names);

            httpRequest.addQueryParam("master_timeout", request.masterNodeTimeout().toString());

            HttpRequestUtils.addIndicesOptions(httpRequest, request);

            httpRequest.execute(new ListenerAsyncCompletionHandler<GetSettingsResponse>(listener) {
                @Override
                protected GetSettingsResponse convert(ResponseWrapper responseWrapper) {
                    return responseWrapper.toGetSettingsResponse();
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
