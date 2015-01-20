package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.optimize.OptimizeAction;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.admin.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.admin.indices.optimize.OptimizeResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class OptimizeActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(OptimizeActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public OptimizeActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public OptimizeAction getAction() {
        return OptimizeAction.INSTANCE;
    }

    public void execute(OptimizeRequest request, final ActionListener<OptimizeResponse> listener) {
        logger.debug("optimize request {}", request);
        try {
            HttpClientImpl httpClient = indicesAdminClient.getHttpClient();

            String indices = HttpRequestUtils.indicesOrAll(request);

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(httpClient.getUrl() + "/" + indices + "/_optimize");
            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest.addQueryParam("upgrade", String.valueOf(request.upgrade()));
            httpRequest.addQueryParam("flush", String.valueOf(request.flush()));
            httpRequest.addQueryParam("max_num_segments", String.valueOf(request.maxNumSegments()));
            httpRequest.addQueryParam("only_expunge_deletes", String.valueOf(request.onlyExpungeDeletes()));
            httpRequest.addQueryParam("wait_for_merge", String.valueOf(request.waitForMerge()));

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<OptimizeResponse>(listener) {
                        @Override
                        // TODO 404 if not exists
                        protected OptimizeResponse convert(Response response) {
                            return OptimizeResponse.parse(response);
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
