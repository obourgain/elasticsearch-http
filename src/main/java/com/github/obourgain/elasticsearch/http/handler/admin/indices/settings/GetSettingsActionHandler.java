package com.github.obourgain.elasticsearch.http.handler.admin.indices.settings;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.admin.indices.settings.GetSettingsResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class GetSettingsActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GetSettingsActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public GetSettingsActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public GetSettingsAction getAction() {
        return GetSettingsAction.INSTANCE;
    }

    public void execute(GetSettingsRequest request, final ActionListener<GetSettingsResponse> listener) {
        logger.debug("get settings request {}", request);
        try {
            // lots of url patterns are accepted, but this one is the most practical for a generic impl
            String indices = HttpRequestUtils.indicesOrAll(request);
            RequestUriBuilder uriBuilder = new RequestUriBuilder(indices);

            String names = Strings.arrayToCommaDelimitedString(request.names());
            if (!names.isEmpty()) {
                names = "/" + names;
            }
            uriBuilder.addEndpoint("_settings" + names);

            uriBuilder.addQueryParameter("master_timeout", request.masterNodeTimeout().toString())
                    .addIndicesOptions(request);

            indicesAdminClient.getHttpClient().submit(HttpClientRequest.createGet(uriBuilder.toString()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<GetSettingsResponse>>() {
                        @Override
                        public Observable<GetSettingsResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<GetSettingsResponse>>() {
                                @Override
                                public Observable<GetSettingsResponse> call(ByteBuf byteBuf) {
                                    return GetSettingsResponse.parse(byteBuf);
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
