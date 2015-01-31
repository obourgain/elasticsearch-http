package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import static com.github.obourgain.elasticsearch.http.response.ValidStatusCodes._404;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.admin.indices.exists.IndicesExistsResponse;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class IndicesExistsActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(IndicesExistsActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public IndicesExistsActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public IndicesExistsAction getAction() {
        return IndicesExistsAction.INSTANCE;
    }

    public void execute(IndicesExistsRequest request, final ActionListener<IndicesExistsResponse> listener) {
        logger.debug("indices exists request {}", request);
        try {
            RequestUriBuilder uriBuilder = new RequestUriBuilder(Strings.arrayToCommaDelimitedString(request.indices()));

            uriBuilder.addQueryParameter("local", request.local());
            uriBuilder.addIndicesOptions(request);

            indicesAdminClient.getHttpClient().client.submit(HttpClientRequest.<ByteBuf>create(HttpMethod.HEAD, uriBuilder.toString()))
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                        @Override
                        public Observable<HttpClientResponse<ByteBuf>> call(HttpClientResponse<ByteBuf> response) {
                            return ErrorHandler.checkError(response, _404);
                        }
                    })
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<IndicesExistsResponse>>() {
                        @Override
                        public Observable<IndicesExistsResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return IndicesExistsResponse.parse(response.getStatus().code());
                        }
                    })
                    .single()
                    .subscribe(new ListenerCompleterObserver<>(listener));

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
