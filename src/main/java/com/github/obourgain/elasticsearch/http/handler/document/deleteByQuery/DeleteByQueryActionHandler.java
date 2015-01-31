package com.github.obourgain.elasticsearch.http.handler.document.deleteByQuery;

import static com.github.obourgain.elasticsearch.http.request.HttpRequestUtils.indicesOrAll;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestAccessor;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class DeleteByQueryActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(DeleteByQueryActionHandler.class);

    private final HttpClient httpClient;

    public DeleteByQueryActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public DeleteByQueryAction getAction() {
        return DeleteByQueryAction.INSTANCE;
    }

    public void execute(DeleteByQueryRequest request, final ActionListener<DeleteByQueryResponse> listener) {
        logger.debug("delete by query request {}", request);
        try {
            String indices = indicesOrAll(request);

            RequestUriBuilder uriBuilder;
            String[] requestTypes = DeleteByQueryRequestAccessor.types(request);
            if (requestTypes != null && requestTypes.length != 0) {
                uriBuilder = new RequestUriBuilder(indices, Strings.arrayToCommaDelimitedString(requestTypes));
            } else {
                uriBuilder = new RequestUriBuilder(indices);
            }
            uriBuilder.addEndpoint("_query");

            uriBuilder.addIndicesOptions(request);

            // for search requests, this can be a String[] but the SearchRequests does the conversion to comma delimited string
            uriBuilder.addQueryParameterIfNotNull("routing", request.routing());

            uriBuilder.addConsistencyLevel(request.consistencyLevel());
            uriBuilder.addReplicationType(request.replicationType());

            if (request.timeout() != ShardReplicationOperationRequest.DEFAULT_TIMEOUT) {
                uriBuilder.addQueryParameter("timeout", request.timeout().toString());
            }

            httpClient.client.submit(HttpClientRequest.createDelete(uriBuilder.toString())
                    .withContent(DeleteByQueryRequestAccessor.getSource(request).toBytes()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<DeleteByQueryResponse>>() {
                        @Override
                        public Observable<DeleteByQueryResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<DeleteByQueryResponse>>() {
                                @Override
                                public Observable<DeleteByQueryResponse> call(ByteBuf byteBuf) {
                                    return DeleteByQueryResponseParser.parse(byteBuf);
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
