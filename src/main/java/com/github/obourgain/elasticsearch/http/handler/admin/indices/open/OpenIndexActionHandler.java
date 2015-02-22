package com.github.obourgain.elasticsearch.http.handler.admin.indices.open;

import static com.github.obourgain.elasticsearch.http.response.ErrorHandler.HANDLES_404;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequestAccessor;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.admin.indices.open.OpenIndexResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

public class OpenIndexActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(OpenIndexActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public OpenIndexActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public OpenIndexAction getAction() {
        return OpenIndexAction.INSTANCE;
    }

    public void execute(OpenIndexRequest request, final ActionListener<OpenIndexResponse> listener) {
        logger.debug("open index request {}", request);
        try {
            RequestUriBuilder uriBuilder;
            if (OpenIndexRequestAccessor.indices(request).length != 0) {
                String indices = Strings.arrayToCommaDelimitedString(OpenIndexRequestAccessor.indices(request));
                uriBuilder = new RequestUriBuilder(indices).addEndpoint("_open");
            } else {
                uriBuilder = new RequestUriBuilder().addEndpoint("_open");
            }

            uriBuilder.addIndicesOptions(request)
                    .addQueryParameter("timeout", request.timeout().toString())
                    .addQueryParameter("master_timeout", request.masterNodeTimeout().toString());

            indicesAdminClient.getHttpClient().submit(HttpClientRequest.createPost(uriBuilder.toString()))
                    .flatMap(HANDLES_404)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<OpenIndexResponse>>() {
                        @Override
                        public Observable<OpenIndexResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<OpenIndexResponse>>() {
                                @Override
                                public Observable<OpenIndexResponse> call(ByteBuf byteBuf) {
                                    return OpenIndexResponse.parse(byteBuf, response.getStatus().code());
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
