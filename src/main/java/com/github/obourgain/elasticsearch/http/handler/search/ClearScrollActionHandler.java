package com.github.obourgain.elasticsearch.http.handler.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hppc.IntOpenHashSet;
import org.elasticsearch.common.hppc.IntSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.response.search.clearscroll.ClearScrollResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class ClearScrollActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(SearchScrollActionHandler.class);
    public static final IntOpenHashSet NON_2xx_VALID_STATUSES = new IntOpenHashSet();
    static {
        NON_2xx_VALID_STATUSES.add(404);
    }

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
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareDelete(httpClient.getUrl() + "/_search/scroll");

            httpRequest.addQueryParam("scroll_id", Strings.collectionToCommaDelimitedString(request.getScrollIds()));

            httpRequest.execute(new ListenerAsyncCompletionHandler<ClearScrollResponse>(listener) {
                @Override
                protected ClearScrollResponse convert(Response response) {
                    return ClearScrollResponse.parse(response);
                }

                @Override
                protected IntSet non200ValidStatuses() {
                    return NON_2xx_VALID_STATUSES;
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
