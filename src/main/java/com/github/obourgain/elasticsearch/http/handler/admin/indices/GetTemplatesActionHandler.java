package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClient;
import com.github.obourgain.elasticsearch.http.admin.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class GetTemplatesActionHandler implements ActionHandler<GetIndexTemplatesRequest, GetIndexTemplatesResponse, GetIndexTemplatesRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(GetTemplatesActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public GetTemplatesActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public GetIndexTemplatesAction getAction() {
        return GetIndexTemplatesAction.INSTANCE;
    }

    @Override
    public void execute(GetIndexTemplatesRequest request, final ActionListener<GetIndexTemplatesResponse> listener) {
        logger.debug("get index templates request {}", request);
        try {
            HttpClient httpClient = indicesAdminClient.getHttpClient();
            String names = Strings.arrayToCommaDelimitedString(request.names());
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(httpClient.getUrl() + "/_template/" + names);

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<GetIndexTemplatesResponse>(listener) {
                        @Override
                        protected GetIndexTemplatesResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toGetIndexTemplatesResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
