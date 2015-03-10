package com.github.obourgain.elasticsearch.http.handler.search.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class SearchScrollActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(SearchScrollActionHandler.class);

    private final HttpClient httpClient;

    public SearchScrollActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public SearchScrollAction getAction() {
        return SearchScrollAction.INSTANCE;
    }

    public void execute(SearchScrollRequest request, final ActionListener<SearchResponse> listener) {
        logger.debug("Search scroll request {}", request);
        try {
            ActionRequestValidationException validation = request.validate();
            if(validation != null && !validation.validationErrors().isEmpty()) {
                throw validation;
            }

            RequestUriBuilder uriBuilder = new RequestUriBuilder()
                    .addEndpoint("_search/scroll");

            if (request.scroll() != null) {
                uriBuilder.addQueryParameter("scroll", request.scroll().keepAlive().toString());
            }
            uriBuilder.addQueryParameter("scroll_id", request.scrollId());

            httpClient.client.submit(HttpClientRequest.createGet(uriBuilder.toString()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<SearchResponse>>() {
                        @Override
                        public Observable<SearchResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<SearchResponse>>() {
                                @Override
                                public Observable<SearchResponse> call(ByteBuf byteBuf) {
                                    return SearchResponse.parse(byteBuf);
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
