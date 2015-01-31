package com.github.obourgain.elasticsearch.http.handler.admin.indices.settings;

import java.util.Map;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestAccessor;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.admin.indices.settings.get.UpdateSettingsResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class UpdateSettingsActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(UpdateSettingsActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public UpdateSettingsActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public UpdateSettingsAction getAction() {
        return UpdateSettingsAction.INSTANCE;
    }

    public void execute(UpdateSettingsRequest request, final ActionListener<UpdateSettingsResponse> listener) {
        // TODO test
        logger.debug("update indices settings request {}", request);
        try {
            // TODO this pattern is repeated a lot, better extract it
            String indices = Strings.arrayToCommaDelimitedString(UpdateSettingsRequestAccessor.indices(request));
            if (!indices.isEmpty()) {
                indices = "/" + indices;
            }
            RequestUriBuilder uriBuilder = new RequestUriBuilder(indices).addEndpoint("_settings");

            Settings settings = UpdateSettingsRequestAccessor.settings(request);

            String body = XContentFactory.jsonBuilder().map((Map) settings.getAsMap()).string();

            uriBuilder.addQueryParameter("timeout", String.valueOf(request.timeout()));
            uriBuilder.addQueryParameter("master_timeout", String.valueOf(request.masterNodeTimeout()));
            uriBuilder.addIndicesOptions(request);

            indicesAdminClient.getHttpClient().client.submit(HttpClientRequest.createGet(uriBuilder.toString())
                    .withContent(body))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<UpdateSettingsResponse>>() {
                        @Override
                        public Observable<UpdateSettingsResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<UpdateSettingsResponse>>() {
                                @Override
                                public Observable<UpdateSettingsResponse> call(ByteBuf byteBuf) {
                                    return UpdateSettingsResponse.parse(byteBuf);
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
