package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hppc.IntSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClient;
import com.github.obourgain.elasticsearch.http.admin.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.ValidStatusCodes;
import com.github.obourgain.elasticsearch.http.response.admin.indices.exists.IndicesExistsResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class IndicesExistsActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(IndicesExistsActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public IndicesExistsActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public IndicesExistsAction getAction() {
        return IndicesExistsAction.INSTANCE;
    }

    public void execute(IndicesExistsRequest request, final ActionListener<IndicesExistsResponse> listener) {
        logger.debug("indices exists request {}", request);
        try {
            HttpClient httpClient = indicesAdminClient.getHttpClient();

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareHead(httpClient.getUrl() + "/" + Strings.arrayToCommaDelimitedString(request.indices()));

            httpRequest.addQueryParam("local", String.valueOf(request.local()));
            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<IndicesExistsResponse>(listener) {
                        @Override
                        protected IndicesExistsResponse convert(Response response) {
                            return IndicesExistsResponse.parse(response);
                        }

                        @Override
                        protected IntSet non200ValidStatuses() {
                            return ValidStatusCodes._404;
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
