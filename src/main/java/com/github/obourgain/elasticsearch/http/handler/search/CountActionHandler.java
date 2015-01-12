package com.github.obourgain.elasticsearch.http.handler.search;

import static com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils.indicesOrAll;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.count.CountAction;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestAccessor;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.internal.SearchContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.count.CountResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class CountActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(CountActionHandler.class);

    private final HttpClientImpl httpClient;

    public CountActionHandler(HttpClientImpl httpClient) {
        this.httpClient = httpClient;
    }

    public CountAction getAction() {
        return CountAction.INSTANCE;
    }

    public void execute(CountRequest request, final ActionListener<CountResponse> listener) {
        logger.debug("count request {}", request);
        try {
            StringBuilder url = new StringBuilder(httpClient.getUrl()).append("/");

            String indices = indicesOrAll(request);
            url.append(indices);

            if (request.types() != null && request.types().length > 0) {
                url.append("/").append(Strings.arrayToCommaDelimitedString(request.types()));
            }
            url.append("/_count");

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(url.toString());
            if(CountRequestAccessor.getMinScore(request) != CountRequest.DEFAULT_MIN_SCORE) {
                httpRequest.addQueryParam("min_score", String.valueOf(CountRequestAccessor.getMinScore(request)));
            }
            if(request.preference() != null) {
                httpRequest.addQueryParam("preference", request.preference());
            }
            if(request.routing() != null) {
                httpRequest.addQueryParam("routing", request.routing());
            }
            if(request.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER) {
                httpRequest.addQueryParam("terminate_after", String.valueOf(request.terminateAfter()));
            }
            BytesReference source = CountRequestAccessor.getSource(request);
            if(source != null) {
                httpRequest.setBody(source.toBytes());
            }
            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<CountResponse>(listener) {
                        @Override
                        protected CountResponse convert(Response response) {
                            return CountResponse.parse(response);
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
