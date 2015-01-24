package com.github.obourgain.elasticsearch.http.handler.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class SearchScrollActionHandler implements ActionHandler<SearchScrollRequest, SearchResponse, SearchScrollRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(SearchScrollActionHandler.class);

    private final HttpClient httpClient;

    public SearchScrollActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public SearchScrollAction getAction() {
        return SearchScrollAction.INSTANCE;
    }

    @Override
    public void execute(SearchScrollRequest request, final ActionListener<SearchResponse> listener) {
        logger.debug("Search scroll request {}", request);
        try {
            // TODO test
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(httpClient.getUrl() + "/_search/scroll");

            if (request.scroll() != null) {
                httpRequest.addQueryParam("scroll", String.valueOf(request.scroll().keepAlive().toString()));
            }
            httpRequest.addQueryParam("scroll_id", request.scrollId());

            httpRequest.execute(new ListenerAsyncCompletionHandler<SearchResponse>(listener) {
                @Override
                protected SearchResponse convert(ResponseWrapper responseWrapper) {
                    return responseWrapper.toSearchResponse();
                }

                @Override
                public void onThrowable(Throwable t) {
                    super.onThrowable(t);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
