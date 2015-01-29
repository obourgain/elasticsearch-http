package com.github.obourgain.elasticsearch.http.handler.search;

import static com.github.obourgain.elasticsearch.http.request.HttpRequestUtils.indicesOrAll;
import static com.github.obourgain.elasticsearch.http.response.ValidStatusCodes._404;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.count.CountAction;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestAccessor;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.internal.SearchContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.search.count.CountResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class CountActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(CountActionHandler.class);

    private final HttpClient httpClient;

    public CountActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public CountAction getAction() {
        return CountAction.INSTANCE;
    }

    public void execute(CountRequest request, final ActionListener<CountResponse> listener) {
        logger.debug("count request {}", request);
        try {
            String indices = indicesOrAll(request);
            RequestUriBuilder uriBuilder;
            if (request.types() != null && request.types().length > 0) {
                String types = Strings.arrayToCommaDelimitedString(request.types());
                uriBuilder = new RequestUriBuilder(indices, types);
            } else {
                uriBuilder = new RequestUriBuilder(indices);
            }
            uriBuilder.addEndpoint("_count");

            if (CountRequestAccessor.getMinScore(request) != CountRequest.DEFAULT_MIN_SCORE) {
                uriBuilder.addQueryParameter("min_score", CountRequestAccessor.getMinScore(request));
            }
            uriBuilder.addQueryParameterIfNotNull("preference", request.preference());
            uriBuilder.addQueryParameterIfNotNull("routing", request.routing());

            if (request.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER) {
                uriBuilder.addQueryParameter("terminate_after", request.terminateAfter());
            }
            uriBuilder.addIndicesOptions(request);

            HttpClientRequest<ByteBuf> httpRequest = HttpClientRequest.createPost(uriBuilder.toString());
            BytesReference source = CountRequestAccessor.getSource(request);
            if (source != null) {
                httpRequest.withContent(source.toBytes());
            }

            httpClient.client.submit(httpRequest)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<HttpClientResponse<ByteBuf>>>() {
                        @Override
                        public Observable<HttpClientResponse<ByteBuf>> call(HttpClientResponse<ByteBuf> response) {
                            return ErrorHandler.checkError(response, _404);
                        }
                    })
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<CountResponse>>() {
                        @Override
                        public Observable<CountResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<CountResponse>>() {
                                @Override
                                public Observable<CountResponse> call(ByteBuf byteBuf) {
                                    return CountResponse.parse(byteBuf);
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
