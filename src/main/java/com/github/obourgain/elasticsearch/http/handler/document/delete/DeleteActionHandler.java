package com.github.obourgain.elasticsearch.http.handler.document.delete;

import java.net.URLEncoder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class DeleteActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(DeleteActionHandler.class);

    private final HttpClient httpClient;

    public DeleteActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public DeleteAction getAction() {
        return DeleteAction.INSTANCE;
    }

    public void execute(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        logger.debug("delete request " + request);
        try {
            RequestUriBuilder uriBuilder = new RequestUriBuilder(request.index(), request.type(), URLEncoder.encode(request.id(), Charsets.UTF_8.displayName()));

            uriBuilder.addQueryParameterIfNotZero("version", request.version());
            uriBuilder.addQueryParameterIfNotNull("routing", request.routing());

            uriBuilder.addVersionType(request.versionType());
            uriBuilder.addConsistencyLevel(request.consistencyLevel());
            uriBuilder.addReplicationType(request.replicationType());

            if (request.refresh()) {
                uriBuilder.addQueryParameter("refresh", true);
            }
            if (request.timeout() != ShardReplicationOperationRequest.DEFAULT_TIMEOUT) {
                uriBuilder.addQueryParameter("timeout", request.timeout().toString());
            }

            httpClient.client.submit(HttpClientRequest.createDelete(uriBuilder.toString()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<DeleteResponse>>() {
                        @Override
                        public Observable<DeleteResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<DeleteResponse>>() {
                                @Override
                                public Observable<DeleteResponse> call(ByteBuf byteBuf) {
                                    return DeleteResponse.parse(byteBuf);
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
