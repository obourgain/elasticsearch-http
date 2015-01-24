package com.github.obourgain.elasticsearch.http.handler.document;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.document.bulk.BulkResponse;
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
public class BulkActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(BulkActionHandler.class);

    private final HttpClient httpClient;

    public BulkActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public BulkAction getAction() {
        return BulkAction.INSTANCE;
    }

    public void execute(final BulkRequest request, final ActionListener<BulkResponse> listener) {
        // TODO lots of options to test
        logger.debug("bulk request {}", request);
        try {

            // probably don't care of this
//            long estimatedSizeInBytes = request.estimatedSizeInBytes();

            // TODO what is this ?
//            List<Object> payloads = request.payloads();

            RequestUriBuilder uriBuilder = new RequestUriBuilder().addEndpoint("_bulk");

            uriBuilder.addQueryParameter("refresh", String.valueOf(request.refresh()));
            uriBuilder.addQueryParameter("timeout", request.timeout().toString());
            uriBuilder.addConsistencyLevel(request.consistencyLevel());
            uriBuilder.addReplicationType(request.replicationType());

            httpClient.client.submit(HttpClientRequest.createPost(uriBuilder.toString())
                    .withContentSource(Observable.create(new ByteBufOnSubscribe(request))))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<BulkResponse>>() {
                        @Override
                        public Observable<BulkResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<BulkResponse>>() {
                                @Override
                                public Observable<BulkResponse> call(ByteBuf byteBuf) {
                                    return BulkResponse.parse(byteBuf);
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
        private final BulkRequest request;

        public ByteBufOnSubscribe(BulkRequest request) {
            this.request = request;
        }

        @Override
        public void call(final Subscriber<? super ByteBuf> subscriber) {
            subscriber.onStart();

            Observable<byte[]> actions = BulkActionMarshaller.write(request.requests());
            actions.forEach(new Action1<byte[]>() {
                @Override
                public void call(byte[] bytes) {
                    subscriber.onNext(PooledByteBufAllocator.DEFAULT.buffer().writeBytes(bytes));
                }
            });

            // TODO may I reuse the buffer here ? could be a nice optimization
            subscriber.onCompleted();
        }
    }
}