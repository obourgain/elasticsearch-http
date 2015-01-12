package com.github.obourgain.elasticsearch.http.handler.document;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.termvector.TermVectorAction;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.response.termvectors.TermVectorResponse;
import com.github.obourgain.elasticsearch.http.response.termvectors.TermVectorResponseParser;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class TermVectorsActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(TermVectorsActionHandler.class);

    private final HttpClientImpl httpClient;

    public TermVectorsActionHandler(HttpClientImpl httpClient) {
        this.httpClient = httpClient;
    }

    public TermVectorAction getAction() {
        return TermVectorAction.INSTANCE;
    }

    public void execute(TermVectorRequest request, final ActionListener<TermVectorResponse> listener) {
        logger.debug("term vector request {}", request);
        try {
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(httpClient.getUrl() + "/" + request.index() + "/" + request.type() + "/" + request.id() + "/_termvector");

            // TODO test
            if(request.routing() != null) {
                httpRequest.addQueryParam("routing", request.routing());
            }
            if(request.preference() != null) {
                httpRequest.addQueryParam("preference", request.preference());
            }

            if(request.selectedFields() != null) {
                httpRequest.addQueryParam("fields", Strings.collectionToCommaDelimitedString(request.selectedFields()));
            }

            httpRequest.addQueryParam("offsets", String.valueOf(request.offsets()));
            httpRequest.addQueryParam("positions", String.valueOf(request.positions()));
            httpRequest.addQueryParam("payloads", String.valueOf(request.payloads()));
            httpRequest.addQueryParam("term_statistics", String.valueOf(request.termStatistics()));
            httpRequest.addQueryParam("field_statistics", String.valueOf(request.fieldStatistics()));

            httpRequest.execute(new ListenerAsyncCompletionHandler<TermVectorResponse>(listener) {
                @Override
                protected TermVectorResponse convert(Response response) {
                    return TermVectorResponseParser.parse(response);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
