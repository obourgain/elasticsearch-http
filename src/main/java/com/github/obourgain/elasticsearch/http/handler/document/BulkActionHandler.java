package com.github.obourgain.elasticsearch.http.handler.document;

import java.io.IOException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Body;
import com.ning.http.client.BodyGenerator;

/**
 * @author olivier bourgain
 */
public class BulkActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(BulkActionHandler.class);

    private final HttpClient httpClient;

    public BulkActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public BulkAction getAction() {
        return BulkAction.INSTANCE;
    }

    public void execute(final BulkRequest request, final ActionListener<BulkResponse> listener) {
        logger.debug("bulk request {}", request);
        try {

            // probably don't care of this
//            long estimatedSizeInBytes = request.estimatedSizeInBytes();

            // TODO what is this ?
//            List<Object> payloads = request.payloads();

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(httpClient.getUrl() + "/_bulk");

//            httpRequest.setBody(writer(request.requests()));

            httpRequest.addQueryParam("refresh", String.valueOf(request.refresh()));
            httpRequest.addQueryParam("timeout", request.timeout().toString());
            switch (request.consistencyLevel()) {
                case ALL:
                case ONE:
                case QUORUM:
                    httpRequest.addQueryParam("consistency", request.consistencyLevel().toString());
                    break;
                case DEFAULT: // no op
                    break;
                default:
                    throw new IllegalStateException("consistency level " + request.consistencyLevel() + " is not supported");
            }
            switch (request.replicationType()) {
                case SYNC:
                case ASYNC:
                    httpRequest.addQueryParam("replication", request.replicationType().toString());
                    break;
                case DEFAULT: // no op
                    break;
                default:
                    throw new IllegalStateException("replication type " + request.replicationType() + " is not supported");
            }

            httpRequest.setBody(bodyGenerator(request));

            httpRequest.execute(new ListenerAsyncCompletionHandler<BulkResponse>(listener) {
                @Override
                protected BulkResponse convert(ResponseWrapper responseWrapper) {
                    // TODO
                    return responseWrapper.toBulkResponse();
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private BodyGenerator bodyGenerator(final BulkRequest request) {
        return new BodyGenerator() {
            @Override
            public Body createBody() throws IOException {
                return new BulkBody(new BulkActionMarshaller(request));
            }
        };
    }
}