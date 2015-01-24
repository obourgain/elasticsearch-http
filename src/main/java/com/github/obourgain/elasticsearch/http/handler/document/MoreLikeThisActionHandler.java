package com.github.obourgain.elasticsearch.http.handler.document;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.mlt.MoreLikeThisAction;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.search.search.SearchResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class MoreLikeThisActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(MoreLikeThisActionHandler.class);

    private final HttpClient httpClient;

    public MoreLikeThisActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public MoreLikeThisAction getAction() {
        return MoreLikeThisAction.INSTANCE;
    }

    public void execute(final MoreLikeThisRequest request, final ActionListener<SearchResponse> listener) {
        logger.debug("MoreLikeThis request {}", request);
        try {
            RequestUriBuilder uriBuilder = new RequestUriBuilder(request.index(), request.type(), request.id()).addEndpoint("_mlt");

            uriBuilder.addQueryParameterArrayAsCommaDelimitedIfNotNullNorEmpty("fields", request.fields());
            uriBuilder.addQueryParameterIfNotNull("routing", request.routing());
            if (request.percentTermsToMatch() != -1) {
                uriBuilder.addQueryParameter("percent_terms_to_match", String.valueOf(request.percentTermsToMatch()));
            }

            if (request.minTermFreq() != -1) {
                uriBuilder.addQueryParameter("min_term_freq", String.valueOf(request.minTermFreq()));
            }
            if (request.maxQueryTerms() != -1) {
                uriBuilder.addQueryParameter("max_query_terms", String.valueOf(request.maxQueryTerms()));
            }

            uriBuilder.addQueryParameterArrayAsCommaDelimitedIfNotNullNorEmpty("stop_words", request.stopWords());

            if (request.minDocFreq() != -1) {
                uriBuilder.addQueryParameter("min_doc_freq", String.valueOf(request.minDocFreq()));
            }
            if (request.maxDocFreq() != -1) {
                uriBuilder.addQueryParameter("max_doc_freq", String.valueOf(request.maxDocFreq()));
            }

            if (request.minWordLength() != -1) {
                uriBuilder.addQueryParameter("min_word_len", String.valueOf(request.minWordLength()));
            }
            if (request.maxWordLength() != -1) {
                uriBuilder.addQueryParameter("max_word_len", String.valueOf(request.maxWordLength()));
            }

            if (request.boostTerms() != -1) {
                uriBuilder.addQueryParameter("boost_terms", String.valueOf(request.boostTerms()));
            }

            uriBuilder.addQueryParameterArrayAsCommaDelimitedIfNotNullNorEmpty("search_indices", request.searchIndices());
            uriBuilder.addQueryParameterArrayAsCommaDelimitedIfNotNullNorEmpty("search_types", request.searchTypes());

            if (request.searchSize() != 0) {
                uriBuilder.addQueryParameter("search_size", String.valueOf(request.searchSize()));
            }
            if(request.searchScroll() != null) {
                uriBuilder.addQueryParameterIfNotNull("search_scroll", request.searchScroll().keepAlive().toString());
            }
            if (request.searchFrom() != 0) {
                uriBuilder.addQueryParameter("search_from", String.valueOf(request.searchFrom()));
            }
            if (request.include()) {
                uriBuilder.addQueryParameter("include", request.include());
            }
            uriBuilder.addQueryParameterArrayAsCommaDelimitedIfNotNullNorEmpty("mlt_fields", request.fields());
            switch (request.searchType()) {
                case COUNT:
                case QUERY_AND_FETCH:
                case QUERY_THEN_FETCH:
                case DFS_QUERY_AND_FETCH:
                case DFS_QUERY_THEN_FETCH:
                case SCAN:
                    uriBuilder.addQueryParameter("search_type", request.searchType().name().toLowerCase());
                    break;
                default:
                    throw new IllegalStateException("search_type " + request.searchType() + " is not supported");
            }

            // TODO it is almost like a search request, add all options

            HttpClientRequest<ByteBuf> get = HttpClientRequest.createGet(uriBuilder.toString());
            if (request.searchSource() != null) {
                get.withContent(request.searchSource().toBytes());
            }

            httpClient.client.submit(get)
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
