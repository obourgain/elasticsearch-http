package com.github.obourgain.elasticsearch.http.response;

import static com.github.obourgain.elasticsearch.http.response.ValidStatusCodes._404;
import java.util.Collections;
import java.util.Set;
import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

public class ErrorHandler {

    public static Func1<HttpClientResponse<ByteBuf>, Observable<HttpClientResponse<ByteBuf>>> AS_FUNC = new Func1<HttpClientResponse<ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
        @Override
        public Observable<HttpClientResponse<ByteBuf>> call(HttpClientResponse<ByteBuf> response) {
            return ErrorHandler.checkError(response);
        }
    };

    public static Func1<HttpClientResponse<ByteBuf>, Observable<HttpClientResponse<ByteBuf>>> HANDLES_404 = new Func1<HttpClientResponse<ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
        @Override
        public Observable<HttpClientResponse<ByteBuf>> call(HttpClientResponse<ByteBuf> response) {
            return ErrorHandler.checkError(response, _404);
        }
    };

    public static Observable<HttpClientResponse<ByteBuf>> checkError(HttpClientResponse<ByteBuf> response) {
        return checkError(response, Collections.<Integer>emptySet());
    }

    public static Observable<HttpClientResponse<ByteBuf>> checkError(HttpClientResponse<ByteBuf> response, Set<Integer> non200ValidStatuses) {
        final int statusCode = response.getStatus().code();
        if (statusCode > 300 && !non200ValidStatuses.contains(statusCode)) {
            return response.getContent().flatMap(new Func1<ByteBuf, Observable<HttpClientResponse<ByteBuf>>>() {
                @Override
                public Observable<HttpClientResponse<ByteBuf>> call(ByteBuf byteBuf) {
                    return Observable.error(new ElasticsearchHttpException(byteBuf.toString(Charsets.UTF_8), statusCode));
                }
            });
        }
        return Observable.just(response);
    }
}
