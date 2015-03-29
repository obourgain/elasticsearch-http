package com.github.obourgain.elasticsearch.http.handler.search.multipercolate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.percolate.MultiPercolateRequest;
import org.elasticsearch.action.percolate.PercolateAction;
import org.elasticsearch.common.bytes.BytesReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.handler.search.percolate.PercolateActionHandler;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class MultiPercolateActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(PercolateActionHandler.class);

    private final HttpClient httpClient;

    public MultiPercolateActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public PercolateAction getAction() {
        return PercolateAction.INSTANCE;
    }

    public void execute(MultiPercolateRequest request, final ActionListener<MultiPercolateResponse> listener) {
        logger.debug("multi percolate request {}", request);

        // no id on PercolateRequest, so I can't do it ...

        String indices = HttpRequestUtils.indicesOrAll(request.indices());
        String type = request.documentType();

        RequestUriBuilder uriBuilder = request.documentType() != null ? new RequestUriBuilder(indices, type) : new RequestUriBuilder(indices);

        uriBuilder.addEndpoint("_mpercolate").addIndicesOptions(request.indicesOptions());

        try {
            HttpClientRequest<ByteBuf> httpRequest = HttpClientRequest.createGet(uriBuilder.toString());
            httpRequest.withContentSource(Observable.create(new ByteBufOnSubscribe(request)));

            httpClient.client.submit(httpRequest)
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<MultiPercolateResponse>>() {
                        @Override
                        public Observable<MultiPercolateResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<MultiPercolateResponse>>() {
                                @Override
                                public Observable<MultiPercolateResponse> call(ByteBuf byteBuf) {
                                    return MultiPercolateResponse.parse(byteBuf);
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

    private static class ByteBufOnSubscribe implements Observable.OnSubscribe<ByteBuf> {
        private final MultiPercolateRequest request;

        public ByteBufOnSubscribe(MultiPercolateRequest request) {
            this.request = request;
        }

        @Override
        public void call(final Subscriber<? super ByteBuf> subscriber) {
            subscriber.onStart();

            Observable<BytesReference> actions = PercolateRequestsMarshaller.lazyConvertToBytes(request.requests());
            actions.forEach(new Action1<BytesReference>() {
                @Override
                public void call(BytesReference bytes) {
                    subscriber.onNext(PooledByteBufAllocator.DEFAULT.buffer().writeBytes(bytes.toBytes()));
                }
            });
            subscriber.onCompleted();
        }
    }
}
