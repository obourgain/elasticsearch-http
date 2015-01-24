package com.github.obourgain.elasticsearch.http.handler.admin.indices.close;

import static com.github.obourgain.elasticsearch.http.response.ValidStatusCodes._404;
import java.util.Set;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequestAccessor;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.admin.indices.close.CloseIndexResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

public class CloseIndexActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(CloseIndexActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public CloseIndexActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public CloseIndexAction getAction() {
        return CloseIndexAction.INSTANCE;
    }

    public void execute(CloseIndexRequest request, final ActionListener<CloseIndexResponse> listener) {
        logger.debug("close index request {}", request);
        try {
            String indices = Strings.arrayToCommaDelimitedString(CloseIndexRequestAccessor.indices(request));
            if (!indices.isEmpty()) {
                indices = "/" + indices;
            }

            HttpClient httpClient = indicesAdminClient.getHttpClient();
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(httpClient.getUrl() + indices + "/_close");

            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest.addQueryParam("timeout", String.valueOf(request.timeout()));
            httpRequest.addQueryParam("master_timeout", String.valueOf(request.masterNodeTimeout()));

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<CloseIndexResponse>(listener) {
                        @Override
                        protected CloseIndexResponse convert(Response response) {
                            return CloseIndexResponse.parse(response);
                        }

                        @Override
                        protected Set<Integer> non200ValidStatuses() {
                            return _404;
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
