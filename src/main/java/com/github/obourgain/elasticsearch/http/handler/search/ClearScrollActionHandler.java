package com.github.obourgain.elasticsearch.http.handler.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class ClearScrollActionHandler implements ActionHandler<ClearScrollRequest, ClearScrollResponse, ClearScrollRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(SearchScrollActionHandler.class);

    private final HttpClient httpClient;

    public ClearScrollActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public ClearScrollAction getAction() {
        return ClearScrollAction.INSTANCE;
    }

    @Override
    public void execute(ClearScrollRequest request, final ActionListener<ClearScrollResponse> listener) {
        logger.debug("clear scroll request {}", request);
        try {
            // TODO test

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareDelete(httpClient.getUrl() + "/_search/scroll");

            httpRequest.addQueryParam("scroll_id", Strings.collectionToCommaDelimitedString(request.getScrollIds()));

            httpRequest.execute(new ListenerAsyncCompletionHandler<ClearScrollResponse>(listener) {
                @Override
                protected ClearScrollResponse convert(ResponseWrapper responseWrapper) {
                    return responseWrapper.toClearScrollResponse();
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
