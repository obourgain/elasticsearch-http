package com.github.obourgain.elasticsearch.http.handler.document.update;

import java.net.URLEncoder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.lucene.uid.Versions;
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
public class UpdateActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(UpdateActionHandler.class);

    private final HttpClient httpClient;

    public UpdateActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public UpdateAction getAction() {
        return UpdateAction.INSTANCE;
    }

    public void execute(UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        // TODO scripted_upsert
        logger.debug("update request {}", request);
        try {
            RequestUriBuilder uriBuilder = new RequestUriBuilder(request.index(), request.type(), URLEncoder.encode(request.id(), Charsets.UTF_8.displayName())).addEndpoint("_update");


            buildRequest(request, uriBuilder);

            httpClient.client.submit(
                    HttpClientRequest.createPost(uriBuilder.toString())
                            .withContent(UpdateHelper.buildRequestBody(request)))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<UpdateResponse>>() {
                        @Override
                        public Observable<UpdateResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<UpdateResponse>>() {
                                @Override
                                public Observable<UpdateResponse> call(ByteBuf byteBuf) {
                                    return UpdateResponse.parse(byteBuf, response.getStatus().code());
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

    public static void buildRequest(UpdateRequest request, final RequestUriBuilder uriBuilder) {
        if (request.version() != Versions.MATCH_ANY) {
            uriBuilder.addQueryParameter("version", request.version());
        }
        uriBuilder.addVersionType(request.versionType());
        uriBuilder.addQueryParameterIfNotNull("lang", request.scriptLang());
        uriBuilder.addQueryParameterIfNotNull("routing", request.routing());

        uriBuilder.addConsistencyLevel(request.consistencyLevel());
        uriBuilder.addReplicationType(request.replicationType());

        uriBuilder.addQueryParameterArrayAsCommaDelimitedIfNotNullNorEmpty("fields", request.fields());
        if (request.refresh()) {
            uriBuilder.addQueryParameter("refresh", true);
        }
        if (request.timeout() != ShardReplicationOperationRequest.DEFAULT_TIMEOUT) {
            uriBuilder.addQueryParameter("timeout", request.timeout().toString());
        }
        uriBuilder.addQueryParameterIfNotZero("retry_on_conflict", request.retryOnConflict());
    }

}
