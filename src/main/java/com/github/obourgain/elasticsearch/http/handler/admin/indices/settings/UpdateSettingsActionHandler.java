package com.github.obourgain.elasticsearch.http.handler.admin.indices.settings;

import java.util.Map;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestAccessor;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
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
public class UpdateSettingsActionHandler implements ActionHandler<UpdateSettingsRequest, UpdateSettingsResponse, UpdateSettingsRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(UpdateSettingsActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public UpdateSettingsActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public UpdateSettingsAction getAction() {
        return UpdateSettingsAction.INSTANCE;
    }

    @Override
    public void execute(UpdateSettingsRequest request, final ActionListener<UpdateSettingsResponse> listener) {
        // TODO test
        logger.debug("update indices settings request {}", request);
        try {
            // TODO this pattern is repeated a lot, better extract it
            String indices = Strings.arrayToCommaDelimitedString(UpdateSettingsRequestAccessor.indices(request));
            if(!indices.isEmpty()) {
                indices = "/" + indices;
            }

            HttpClient httpClient = indicesAdminClient.getHttpClient();
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePut(httpClient.getUrl() + indices + "/_settings");

            Settings settings = UpdateSettingsRequestAccessor.settings(request);

            String body = XContentFactory.jsonBuilder().map((Map) settings.getAsMap()).string();

            httpRequest.setBody(body);

            httpRequest.addQueryParam("timeout", String.valueOf(request.timeout()));
            httpRequest.addQueryParam("master_timeout", String.valueOf(request.masterNodeTimeout()));
            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<UpdateSettingsResponse>(listener) {
                        @Override
                        protected UpdateSettingsResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toUpdateSettingsResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
