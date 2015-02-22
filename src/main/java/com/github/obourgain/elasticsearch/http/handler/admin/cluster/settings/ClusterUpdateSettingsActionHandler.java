package com.github.obourgain.elasticsearch.http.handler.admin.cluster.settings;

import java.util.Map;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestAccessor;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClusterAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

public class ClusterUpdateSettingsActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ClusterUpdateSettingsActionHandler.class);

    private final HttpClusterAdminClient indicesAdminClient;

    public ClusterUpdateSettingsActionHandler(HttpClusterAdminClient httpClusterAdminClient) {
        this.indicesAdminClient = httpClusterAdminClient;
    }

    public ClusterUpdateSettingsAction getAction() {
        return ClusterUpdateSettingsAction.INSTANCE;
    }

    public void execute(ClusterUpdateSettingsRequest request, final ActionListener<ClusterUpdateSettingsResponse> listener) {
        // TODO test
        logger.debug("cluster update settings request {}", request);
        try {
            RequestUriBuilder uriBuilder = new RequestUriBuilder().addEndpoint("_cluster/settings");

            // TODO what to do with headers ?
            request.getHeaders();
            Settings transientSettings = ClusterUpdateSettingsRequestAccessor.transientSettings(request);
            Settings persistentSettings = ClusterUpdateSettingsRequestAccessor.persistentSettings(request);

            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
            if (transientSettings != ImmutableSettings.Builder.EMPTY_SETTINGS) {
                // TODO waiting for https://github.com/elasticsearch/elasticsearch/pull/7212 to be merged and use a cleaner API
                xContentBuilder.field("transient").map((Map) transientSettings.getAsMap());
            }
            if (persistentSettings != ImmutableSettings.Builder.EMPTY_SETTINGS) {
                xContentBuilder.field("persistent").map((Map) persistentSettings.getAsMap());
            }
            xContentBuilder.endObject();
            String body = xContentBuilder.string();

            uriBuilder.addQueryParameter("timeout", request.timeout().toString());
            uriBuilder.addQueryParameter("master_timeout", request.masterNodeTimeout().toString());
            uriBuilder.addQueryParameter("flat_settings", true);

            indicesAdminClient.getHttpClient().submit(HttpClientRequest.createPut(uriBuilder.toString())
                    .withContent(body))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<ClusterUpdateSettingsResponse>>() {
                        @Override
                        public Observable<ClusterUpdateSettingsResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<ClusterUpdateSettingsResponse>>() {
                                @Override
                                public Observable<ClusterUpdateSettingsResponse> call(ByteBuf byteBuf) {
                                    // TODO
                                    return null;
                                }
                            });
                        }
                    })
                    .single()
                    .subscribe(new ListenerCompleterObserver<>(listener));

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
