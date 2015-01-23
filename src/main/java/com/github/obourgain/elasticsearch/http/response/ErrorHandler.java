package com.github.obourgain.elasticsearch.http.response;

import java.io.IOException;
import org.elasticsearch.common.hppc.IntOpenHashSet;
import org.elasticsearch.common.hppc.IntSet;
import com.google.common.base.Charsets;
import com.ning.http.client.Response;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

public class ErrorHandler {

    public static final IntSet EMPTY = IntOpenHashSet.newInstance();

    public static void checkError(Response response, IntSet non200ValidStatuses) {
        int statusCode = response.getStatusCode();
        if (statusCode > 300 && !non200ValidStatuses.contains(statusCode)) {
            try {
                throw new ElasticsearchHttpException(response.getResponseBody(), statusCode);
            } catch (IOException e) {
                RuntimeException exception = new RuntimeException("Unable to read response", e);
                exception.addSuppressed(new ElasticsearchHttpException(statusCode));
                throw exception;
            }
        }
    }

    public static Observable<HttpClientResponse<ByteBuf>> checkError(HttpClientResponse<ByteBuf> response) {
        return checkError(response, EMPTY);
    }

    public static Observable<HttpClientResponse<ByteBuf>> checkError(HttpClientResponse<ByteBuf> response, IntSet non200ValidStatuses) {
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
