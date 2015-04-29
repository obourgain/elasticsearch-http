package com.github.obourgain.elasticsearch.http.handler.search.search;

import static com.github.obourgain.elasticsearch.http.request.HttpRequestUtils.indicesOrAll;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
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
public class SearchActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(SearchActionHandler.class);

    private final HttpClient httpClient;

    public SearchActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public SearchAction getAction() {
        return SearchAction.INSTANCE;
    }

    public void execute(SearchRequest request, final ActionListener<SearchResponse> listener) {
        logger.debug("search request {}", request);
        try {
            RequestUriBuilder uriBuilder;
            if (request.types() != null && request.types().length > 0) {
                uriBuilder = new RequestUriBuilder(indicesOrAll(request), Strings.arrayToCommaDelimitedString(request.types()));
            } else {
                uriBuilder = new RequestUriBuilder(indicesOrAll(request));
            }
            uriBuilder.addEndpoint("_search");

            // for search requests, this can be a String[] but the SearchRequests does the conversion to comma delimited string
            uriBuilder.addQueryParameterIfNotNull("routing", request.routing());
            uriBuilder.addQueryParameterIfNotNull("preference", request.preference());
            uriBuilder.addQueryParameterIfNotNull("query_cache", request.queryCache());

            uriBuilder.addSearchType(request.searchType());

            if (request.scroll() != null) {
                uriBuilder.addQueryParameter("scroll", request.scroll().keepAlive().toString());
            }

            uriBuilder.addIndicesOptions(request);

            HttpClientRequest<ByteBuf> get = HttpClientRequest.createPost(uriBuilder.toString());
            if (request.source() != null) {
                get.withContent(request.source().toBytes());
            }

            httpClient.getHttpClient().submit(get)
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
