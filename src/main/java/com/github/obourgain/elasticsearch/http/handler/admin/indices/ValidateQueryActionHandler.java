package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateRequestAccessor;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.response.validate.ValidateQueryResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class ValidateQueryActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ValidateQueryActionHandler.class);

    private final HttpIndicesAdminClient httpClient;

    public ValidateQueryActionHandler(HttpIndicesAdminClient httpClient) {
        this.httpClient = httpClient;
    }

    public ValidateQueryAction getAction() {
        return ValidateQueryAction.INSTANCE;
    }

    public void execute(ValidateQueryRequest request, final ActionListener<ValidateQueryResponse> listener) {
        // TODO test
        logger.debug("validate query request {}", request);
        try {
            StringBuilder url = new StringBuilder(httpClient.getHttpClient().getUrl()).append("/")
                    .append(Strings.arrayToCommaDelimitedString(request.indices()));

            if (request.types() != null) {
                url.append("/").append(Strings.arrayToCommaDelimitedString(request.types()));
            }
            url.append("/_validate/query");

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.getHttpClient().asyncHttpClient.prepareGet(url.toString());

            if (request.explain()) {
                httpRequest.addQueryParam("explain", "true");
            }

            httpRequest
                    .setBody(ValidateRequestAccessor.getSource(request).toBytes())
                    .execute(new ListenerAsyncCompletionHandler<ValidateQueryResponse>(listener) {
                        @Override
                        protected ValidateQueryResponse convert(Response response) {
                            return ValidateQueryResponse.parse(response);
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
