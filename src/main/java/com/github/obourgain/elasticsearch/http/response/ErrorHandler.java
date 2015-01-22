package com.github.obourgain.elasticsearch.http.response;

import java.io.IOException;
import org.elasticsearch.common.hppc.IntOpenHashSet;
import org.elasticsearch.common.hppc.IntSet;
import com.google.common.base.Charsets;
import com.ning.http.client.Response;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.functions.Action1;
import rx.observables.BlockingObservable;

public class ErrorHandler {

    public static final IntSet EMPTY = IntOpenHashSet.newInstance();

    public static void checkError(Response response, IntSet non200ValidStatuses) {
        int statusCode = response.getStatusCode();
        if(statusCode > 300 && !non200ValidStatuses.contains(statusCode)) {
            try {
                throw new ElasticsearchHttpException(response.getResponseBody(), statusCode);
            } catch (IOException e) {
                RuntimeException exception = new RuntimeException("Unable to read response", e);
                exception.addSuppressed(new ElasticsearchHttpException(statusCode));
                throw exception;
            }
        }
    }

    public static void checkError(HttpClientResponse<ByteBuf> response) {
        checkError(response, EMPTY);
    }

    public static void checkError(HttpClientResponse<ByteBuf> response, IntSet non200ValidStatuses) {
        final int statusCode = response.getStatus().code();
        if(statusCode > 300 && !non200ValidStatuses.contains(statusCode)) {
//            try {
            response.getContent().single().forEach(new Action1<ByteBuf>() {
                @Override
                public void call(ByteBuf byteBuf) {
                    throw new ElasticsearchHttpException(byteBuf.toString(Charsets.UTF_8), statusCode);
                }
            });
//            BlockingObservable<ByteBuf> byteBufBlockingObservable = response.getContent().toBlocking();
//            ByteBuf first = byteBufBlockingObservable.first();
//            String message = first.toString(Charsets.UTF_8);
//            throw new ElasticsearchHttpException(message, statusCode);
//                throw new ElasticsearchHttpException(response.getContent(), statusCode);
//            } catch (IOException e) {
//                RuntimeException exception = new RuntimeException("Unable to read response", e);
//                exception.addSuppressed(new ElasticsearchHttpException(statusCode));
//                throw exception;
//            }
        }
    }

}
