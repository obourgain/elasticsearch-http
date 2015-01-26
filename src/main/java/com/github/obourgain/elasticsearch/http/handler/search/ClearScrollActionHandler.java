package com.github.obourgain.elasticsearch.http.handler.search;

import static com.github.obourgain.elasticsearch.http.response.ValidStatusCodes._404;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.search.clearscroll.ClearScrollResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class ClearScrollActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(SearchScrollActionHandler.class);

    private final HttpClient httpClient;

    public ClearScrollActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public ClearScrollAction getAction() {
        return ClearScrollAction.INSTANCE;
    }

    public void execute(ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {
        logger.debug("clear scroll request {}", request);
        try {
            RequestUriBuilder uriBuilder = new RequestUriBuilder()
                    .addEndpoint("_search/scroll");

            uriBuilder.addQueryParameter("scroll_id", Strings.collectionToCommaDelimitedString(request.getScrollIds()));
            httpClient.client.submit(HttpClientRequest.createDelete(uriBuilder.toString()))
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                        @Override
                        public Observable<HttpClientResponse<ByteBuf>> call(HttpClientResponse<ByteBuf> response) {
                            return ErrorHandler.checkError(response, _404);
                        }
                    })
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<ClearScrollResponse>>() {
                        @Override
                        public Observable<ClearScrollResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<ClearScrollResponse>>() {
                                @Override
                                public Observable<ClearScrollResponse> call(ByteBuf byteBuf) {
                                    return ClearScrollResponse.parse(response.getStatus().code());
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
