package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.admin.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class IndicesExistsActionHandler implements ActionHandler<IndicesExistsRequest, IndicesExistsResponse, IndicesExistsRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(IndicesExistsActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public IndicesExistsActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public IndicesExistsAction getAction() {
        return IndicesExistsAction.INSTANCE;
    }

    @Override
    public void execute(IndicesExistsRequest request, final ActionListener<IndicesExistsResponse> listener) {
        logger.debug("indices exists request {}", request);
        try {
            HttpClientImpl httpClient = indicesAdminClient.getHttpClient();

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareHead(httpClient.getUrl() + "/" + Strings.arrayToCommaDelimitedString(request.indices()));

            httpRequest.addQueryParam("local", String.valueOf(request.local()));
            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<IndicesExistsResponse>(listener) {
                        @Override
                        public IndicesExistsResponse onCompleted(Response response) {
                            int statusCode = response.getStatusCode();
                            boolean exists;
                            switch (statusCode) {
                                case 200:
                                    exists = true; break;
                                case 404:
                                    exists = false; break;
                                // TODO when cluster blocks, I get a 403
                                default : throw new IllegalStateException("status code " + statusCode + " is not supported for indices exists request");
                            }
                            IndicesExistsResponse indicesExistsResponse = new IndicesExistsResponse(exists);
                            listener.onResponse(indicesExistsResponse);
                            return indicesExistsResponse;
                        }

                        @Override
                        protected IndicesExistsResponse convert(ResponseWrapper responseWrapper) {
                            throw new IllegalStateException("not implemented for indices exists");
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
