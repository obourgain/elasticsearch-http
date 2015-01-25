package com.github.obourgain.elasticsearch.http.handler.admin.indices.flush;

import static com.github.obourgain.elasticsearch.http.response.ValidStatusCodes._404;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushAction;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.admin.indices.flush.FlushResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class FlushActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(FlushActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public FlushActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public FlushAction getAction() {
        return FlushAction.INSTANCE;
    }

    public void execute(FlushRequest request, final ActionListener<FlushResponse> listener) {
        logger.debug("flush request {}", request);
        try {
            String indices = HttpRequestUtils.indicesOrAll(request);
            RequestUriBuilder uriBuilder = new RequestUriBuilder(indices).addEndpoint("_flush");

            uriBuilder.addIndicesOptions(request);
            uriBuilder.addQueryParameter("force", request.force());
            uriBuilder.addQueryParameter("full", request.full());
            uriBuilder.addQueryParameter("wait_if_ongoing", request.waitIfOngoing());

            indicesAdminClient.getHttpClient().client.submit(HttpClientRequest.createPost(uriBuilder.toString()))
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                        @Override
                        public Observable<HttpClientResponse<ByteBuf>> call(HttpClientResponse<ByteBuf> response) {
                            return ErrorHandler.checkError(response, _404);
                        }
                    })
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<FlushResponse>>() {
                        @Override
                        public Observable<FlushResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<FlushResponse>>() {
                                @Override
                                public Observable<FlushResponse> call(ByteBuf byteBuf) {
                                    return FlushResponse.parse(byteBuf, response.getStatus().code());
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
