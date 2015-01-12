package com.github.obourgain.elasticsearch.http.handler.admin.cluster.settings;

import java.util.Map;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestAccessor;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.admin.HttpClusterAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

public class ClusterUpdateSettingsActionHandler implements ActionHandler<ClusterUpdateSettingsRequest, ClusterUpdateSettingsResponse, ClusterUpdateSettingsRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(ClusterUpdateSettingsActionHandler.class);

    private final HttpClusterAdminClient indicesAdminClient;

    public ClusterUpdateSettingsActionHandler(HttpClusterAdminClient httpClusterAdminClient) {
        this.indicesAdminClient = httpClusterAdminClient;
    }

    @Override
    public ClusterUpdateSettingsAction getAction() {
        return ClusterUpdateSettingsAction.INSTANCE;
    }

    @Override
    public void execute(ClusterUpdateSettingsRequest request, final ActionListener<ClusterUpdateSettingsResponse> listener) {
        // TODO test
        logger.debug("cluster update settings request {}", request);
        try {
            HttpClientImpl httpClient = indicesAdminClient.getHttpClient();
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePut(httpClient.getUrl() + "/_cluster/settings");

            // TODO what to do with headers ?
            request.getHeaders();
            Settings transientSettings = ClusterUpdateSettingsRequestAccessor.transientSettings(request);
            Settings persistentSettings = ClusterUpdateSettingsRequestAccessor.persistentSettings(request);

            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
            if(transientSettings != ImmutableSettings.Builder.EMPTY_SETTINGS) {
                // TODO waiting for https://github.com/elasticsearch/elasticsearch/pull/7212 to be merged and use a cleaner API
                xContentBuilder.field("transient").map((Map) transientSettings.getAsMap());
            }
            if(persistentSettings != ImmutableSettings.Builder.EMPTY_SETTINGS) {
                xContentBuilder.field("persistent").map((Map) persistentSettings.getAsMap());
            }
            xContentBuilder.endObject();
            String body = xContentBuilder.string();

            httpRequest.addQueryParam("timeout", String.valueOf(request.timeout()));
            httpRequest.addQueryParam("master_timeout", String.valueOf(request.masterNodeTimeout()));
            httpRequest.addQueryParam("flat_settings", String.valueOf(true));

            httpRequest
                    .setBody(body)
                    .execute(new ListenerAsyncCompletionHandler<ClusterUpdateSettingsResponse>(request, listener) {
                        @Override
                        protected ClusterUpdateSettingsResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toClusterUpdateSettingsResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
