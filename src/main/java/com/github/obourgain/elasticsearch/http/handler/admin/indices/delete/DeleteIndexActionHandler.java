package com.github.obourgain.elasticsearch.http.handler.admin.indices.delete;

import static com.github.obourgain.elasticsearch.http.response.ValidStatusCodes._404;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestAccessor;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.admin.indices.delete.DeleteIndexResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class DeleteIndexActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(DeleteIndexActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public DeleteIndexActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public DeleteIndexAction getAction() {
        return DeleteIndexAction.INSTANCE;
    }

    public void execute(DeleteIndexRequest request, final ActionListener<DeleteIndexResponse> listener) {
        logger.debug("delete index request {}", request);
        try {
            String[] indices = DeleteIndexRequestAccessor.indices(request);
            if (indices.length == 0) {
                throw new IllegalArgumentException("missing indices");
            }

            RequestUriBuilder uriBuilder = new RequestUriBuilder(Strings.arrayToCommaDelimitedString(indices));

            uriBuilder.addQueryParameter("timeout", request.timeout().toString());
            uriBuilder.addQueryParameter("master_timeout", request.timeout().toString());
            uriBuilder.addIndicesOptions(request);

            indicesAdminClient.getHttpClient().client.submit(HttpClientRequest.createDelete(uriBuilder.toString()))
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                        @Override
                        public Observable<HttpClientResponse<ByteBuf>> call(HttpClientResponse<ByteBuf> response) {
                            return ErrorHandler.checkError(response, _404);
                        }
                    })
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<DeleteIndexResponse>>() {
                        @Override
                        public Observable<DeleteIndexResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<DeleteIndexResponse>>() {
                                @Override
                                public Observable<DeleteIndexResponse> call(ByteBuf byteBuf) {
                                    return DeleteIndexResponse.parse(byteBuf, response.getStatus().code());
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
