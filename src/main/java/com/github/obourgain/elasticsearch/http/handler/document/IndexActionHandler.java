package com.github.obourgain.elasticsearch.http.handler.document;

import java.net.URLEncoder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.lucene.uid.Versions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.document.index.IndexResponse;
import com.github.obourgain.elasticsearch.http.response.document.index.IndexResponseParser;
import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class IndexActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(IndexActionHandler.class);

    private final HttpClient httpClient;

    public IndexActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public IndexAction getAction() {
        return IndexAction.INSTANCE;
    }

    public void execute(IndexRequest request, final ActionListener<IndexResponse> listener) {
        logger.debug("index request {}", request);
        try {
            String method;
            RequestUriBuilder uriBuilder;
            if (request.id() == null || request.id().length() == 0) {
                method = "POST";
                uriBuilder = new RequestUriBuilder(request.index(), request.type());
            } else {
                method = "PUT";
                uriBuilder = new RequestUriBuilder(request.index(), request.type(), URLEncoder.encode(request.id(), Charsets.UTF_8.displayName()));
            }

            if (request.version() != Versions.MATCH_ANY) {
                uriBuilder.addQueryParameter("version", String.valueOf(request.version()));
            }
            uriBuilder.addVersionType(request.versionType());

            if (request.opType() == IndexRequest.OpType.CREATE) {
                uriBuilder.addQueryParameter("op_type", "create");
            }
            uriBuilder.addQueryParameterIfNotNull("routing", request.routing());
            uriBuilder.addQueryParameterIfNotNull("parent", request.parent());
            uriBuilder.addQueryParameterIfNotNull("timestamp", request.timestamp());
            if (request.ttl() != -1) {
                uriBuilder.addQueryParameter("ttl", String.valueOf(request.ttl()));
            }
            uriBuilder.addConsistencyLevel(request.consistencyLevel());
            uriBuilder.addReplicationType(request.replicationType());

            if (request.refresh()) {
                uriBuilder.addQueryParameter("refresh", true);
            }
            if (request.timeout() != ShardReplicationOperationRequest.DEFAULT_TIMEOUT) {
                uriBuilder.addQueryParameter("timeout", request.timeout().toString());
            }

            HttpClientRequest<ByteBuf> httpClientRequest;
            if(method.equals("POST")) {
                httpClientRequest = HttpClientRequest.createPost(uriBuilder.toString());
            } else {
                httpClientRequest = HttpClientRequest.createPut(uriBuilder.toString());
            }
            httpClient.client.submit(httpClientRequest.withContent(request.source().toBytes()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<IndexResponse>>() {
                        @Override
                        public Observable<IndexResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<IndexResponse>>() {
                                @Override
                                public Observable<IndexResponse> call(ByteBuf byteBuf) {
                                    return IndexResponseParser.parse(byteBuf);
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
