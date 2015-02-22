package com.github.obourgain.elasticsearch.http.handler.admin.indices.refresh;

import static com.github.obourgain.elasticsearch.http.response.ErrorHandler.HANDLES_404;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.admin.indices.refresh.RefreshResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class RefreshActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(RefreshActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public RefreshActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public RefreshAction getAction() {
        return RefreshAction.INSTANCE;
    }

    public void execute(RefreshRequest request, final ActionListener<RefreshResponse> listener) {
        logger.debug("refresh request {}", request);
        try {
            String indices = HttpRequestUtils.indicesOrAll(request);
            RequestUriBuilder uriBuilder = new RequestUriBuilder(indices).addEndpoint("_refresh");

            uriBuilder.addIndicesOptions(request)
                    .addQueryParameter("force", request.force());

            indicesAdminClient.getHttpClient().submit(HttpClientRequest.createPost(uriBuilder.toString()))
                    .flatMap(HANDLES_404)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<RefreshResponse>>() {
                        @Override
                        public Observable<RefreshResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<RefreshResponse>>() {
                                @Override
                                public Observable<RefreshResponse> call(ByteBuf byteBuf) {
                                    return RefreshResponse.parse(byteBuf);
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
