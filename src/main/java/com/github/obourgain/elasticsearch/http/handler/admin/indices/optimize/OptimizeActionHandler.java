package com.github.obourgain.elasticsearch.http.handler.admin.indices.optimize;

import static com.github.obourgain.elasticsearch.http.response.ErrorHandler.HANDLES_404;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.optimize.OptimizeAction;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.admin.indices.optimize.OptimizeResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class OptimizeActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(OptimizeActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public OptimizeActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public OptimizeAction getAction() {
        return OptimizeAction.INSTANCE;
    }

    public void execute(OptimizeRequest request, final ActionListener<OptimizeResponse> listener) {
        logger.debug("optimize request {}", request);
        try {
            String indices = HttpRequestUtils.indicesOrAll(request);

            RequestUriBuilder uriBuilder = new RequestUriBuilder(indices)
                    .addEndpoint("_optimize")
                    .addIndicesOptions(request)
                    .addQueryParameter("upgrade", request.upgrade())
                    .addQueryParameter("flush", request.flush())
                    .addQueryParameter("max_num_segments", request.maxNumSegments())
                    .addQueryParameter("only_expunge_deletes", request.onlyExpungeDeletes());

            indicesAdminClient.getHttpClient().submit(HttpClientRequest.createPost(uriBuilder.toString()))
                    .flatMap(HANDLES_404)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<OptimizeResponse>>() {
                        @Override
                        public Observable<OptimizeResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<OptimizeResponse>>() {
                                @Override
                                public Observable<OptimizeResponse> call(ByteBuf byteBuf) {
                                    return OptimizeResponse.parse(byteBuf, response.getStatus().code());
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
