package com.github.obourgain.elasticsearch.http.handler.admin.indices.close;

import static com.github.obourgain.elasticsearch.http.response.ErrorHandler.HANDLES_404;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequestAccessor;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.admin.indices.close.CloseIndexResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

public class CloseIndexActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(CloseIndexActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public CloseIndexActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public CloseIndexAction getAction() {
        return CloseIndexAction.INSTANCE;
    }

    public void execute(final CloseIndexRequest request, final ActionListener<CloseIndexResponse> listener) {
        logger.debug("close index request {}", request);
        try {
            String indices = Strings.arrayToCommaDelimitedString(CloseIndexRequestAccessor.indices(request));
            if (!indices.isEmpty()) {
                indices = "/" + indices;
            }

            RequestUriBuilder uriBuilder = new RequestUriBuilder(indices).addEndpoint("_close");
            uriBuilder.addIndicesOptions(request);

            uriBuilder.addQueryParameter("timeout", request.timeout().toString());
            uriBuilder.addQueryParameter("master_timeout", request.masterNodeTimeout().toString());

            indicesAdminClient.getHttpClient().submit(HttpClientRequest.createPost(uriBuilder.toString()))
                    .flatMap(HANDLES_404)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<CloseIndexResponse>>() {
                        @Override
                        public Observable<CloseIndexResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<CloseIndexResponse>>() {
                                @Override
                                public Observable<CloseIndexResponse> call(ByteBuf byteBuf) {
                                    return CloseIndexResponse.parse(byteBuf, response.getStatus().code());
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
