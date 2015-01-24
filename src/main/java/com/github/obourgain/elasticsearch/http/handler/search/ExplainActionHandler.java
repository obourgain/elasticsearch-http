package com.github.obourgain.elasticsearch.http.handler.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.response.search.explain.ExplainResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class ExplainActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ExplainActionHandler.class);

    private final HttpClient httpClient;

    public ExplainActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public ExplainAction getAction() {
        return ExplainAction.INSTANCE;
    }

    public void execute(ExplainRequest request, final ActionListener<ExplainResponse> listener) {
        logger.debug("explain request {}", request);
        try {
            String url = httpClient.getUrl() + "/" + request.index() + "/" + request.type() + "/" + request.id() + "/_explain";
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(url);

            if(request.fetchSourceContext() != null) {
                if(request.fetchSourceContext().fetchSource()) {
                    httpRequest.addQueryParam("_source", String.valueOf(request.fetchSourceContext().fetchSource()));
                }
                // excludes & includes defaults to empty String array
                if(request.fetchSourceContext().excludes().length > 0) {
                    httpRequest.addQueryParam("_source_exclude", Strings.arrayToCommaDelimitedString(request.fetchSourceContext().excludes()));
                }
                if(request.fetchSourceContext().includes().length > 0) {
                    httpRequest.addQueryParam("_source_include", Strings.arrayToCommaDelimitedString(request.fetchSourceContext().excludes()));
                }
            }

            if(request.fields() != null) {
                httpRequest.addQueryParam("fields", Strings.arrayToCommaDelimitedString(request.fields()));
            }
            if (request.routing() != null) {
                // for search requests, this can be a String[] but the SearchRequests does the conversion to comma delimited string
                httpRequest.addQueryParam("routing", request.routing());
            }
            if (request.preference() != null) {
                httpRequest.addQueryParam("preference", request.preference());
            }

            httpRequest
                    .setBody(request.source().toBytes())
                    .execute(new ListenerAsyncCompletionHandler<ExplainResponse>(listener) {
                        @Override
                        protected ExplainResponse convert(Response response) {
                            return ExplainResponse.parse(response);
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
