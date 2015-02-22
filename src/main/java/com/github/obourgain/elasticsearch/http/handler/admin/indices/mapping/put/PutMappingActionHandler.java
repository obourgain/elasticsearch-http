package com.github.obourgain.elasticsearch.http.handler.admin.indices.mapping.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.admin.indices.mapping.put.PutMappingResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class PutMappingActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(PutMappingActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public PutMappingActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public PutMappingAction getAction() {
        return PutMappingAction.INSTANCE;
    }

    public void execute(PutMappingRequest request, final ActionListener<PutMappingResponse> listener) {
        logger.debug("put mapping request {}", request);
        try {
            String indices = HttpRequestUtils.indicesOrAll(request);

            RequestUriBuilder uriBuilder = new RequestUriBuilder(indices, request.type()).addEndpoint("_mapping");
            uriBuilder.addIndicesOptions(request);

            uriBuilder.addQueryParameter("ignore_conflicts", request.ignoreConflicts());
            uriBuilder.addQueryParameter("timeout", request.timeout().toString());
            uriBuilder.addQueryParameter("master_timeout", request.masterNodeTimeout().toString());

            indicesAdminClient.getHttpClient().submit(HttpClientRequest.createPost(uriBuilder.toString())
                    .withContent(request.source()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<PutMappingResponse>>() {
                        @Override
                        public Observable<PutMappingResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<PutMappingResponse>>() {
                                @Override
                                public Observable<PutMappingResponse> call(ByteBuf byteBuf) {
                                    return PutMappingResponse.parse(byteBuf, response.getStatus().code());
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
