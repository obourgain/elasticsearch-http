package com.github.obourgain.elasticsearch.http.handler.search;

import static com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils.indicesOrAll;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.search.SearchResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class SearchActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(SearchActionHandler.class);

    private final HttpClientImpl httpClient;

    public SearchActionHandler(HttpClientImpl httpClient) {
        this.httpClient = httpClient;
    }

    public SearchAction getAction() {
        return SearchAction.INSTANCE;
    }

    public void execute(SearchRequest request, final ActionListener<SearchResponse> listener) {
        logger.debug("search request {}", request);
        try {
            // TODO http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search.html#stats-groups
            // TODO all options org.elasticsearch.rest.action.search.RestSearchAction.RestSearchAction()
            StringBuilder url = new StringBuilder(httpClient.getUrl()).append("/");

            String indices = indicesOrAll(request);
            url.append(indices);

            if (request.types() != null && request.types().length > 0) {
                url.append("/").append(Strings.arrayToCommaDelimitedString(request.types()));
            }
            url.append("/_search");

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(url.toString());

            if (request.routing() != null) {
                // for search requests, this can be a String[] but the SearchRequests does the conversion to comma delimited string
                httpRequest.addQueryParam("routing", request.routing());
            }
            if (request.preference() != null) {
                httpRequest.addQueryParam("preference", request.preference());
            }
            if (request.queryCache() != null) {
                httpRequest.addQueryParam("query_cache", String.valueOf(request.queryCache()));
            }

            switch (request.searchType()) {
                case COUNT:
                case QUERY_AND_FETCH:
                case QUERY_THEN_FETCH:
                case DFS_QUERY_AND_FETCH:
                case DFS_QUERY_THEN_FETCH:
                case SCAN:
                    httpRequest.addQueryParam("search_type", request.searchType().name().toLowerCase());
                    break;
                default:
                    throw new IllegalStateException("search_type " + request.searchType() + " is not supported");
            }

            if (request.scroll() != null) {
                httpRequest.addQueryParam("scroll", request.scroll().keepAlive().toString());
            }

            HttpRequestUtils.addIndicesOptions(httpRequest, request);

            if(request.source() != null) {
                httpRequest.setBody(request.source().toBytes());
            }
            // TODO how to use extrasource ?
//            XContentHelper.convertToJson(request.extraSource(), false);

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<SearchResponse>(listener) {
                        @Override
                        protected SearchResponse convert(Response response) {
                            return SearchResponse.parse(response);
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
