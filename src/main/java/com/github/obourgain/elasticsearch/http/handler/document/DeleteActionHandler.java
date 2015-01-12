package com.github.obourgain.elasticsearch.http.handler.document;

import java.net.URLEncoder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.response.delete.DeleteResponse;
import com.github.obourgain.elasticsearch.http.response.delete.DeleteResponseParser;
import com.google.common.base.Charsets;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class DeleteActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(DeleteActionHandler.class);

    private final HttpClientImpl httpClient;

    public DeleteActionHandler(HttpClientImpl httpClient) {
        this.httpClient = httpClient;
    }

    public DeleteAction getAction() {
        return DeleteAction.INSTANCE;
    }

    public void execute(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        logger.debug("delete request " + request);
        try {
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareDelete(httpClient.getUrl() + "/" + request.index() + "/" + request.type() + "/" + URLEncoder.encode(request.id(), Charsets.UTF_8.displayName()));

            if(request.version() != 0) {
                httpRequest.addQueryParam("version", String.valueOf(request.version()));
            }
            if(request.routing() != null) {
                httpRequest.addQueryParam("routing", request.routing());
            }

            switch (request.versionType().name()) {
                case "EXTERNAL":
                case "EXTERNAL_GTE":
                case "EXTERNAL_GT":
                case "FORCE":
                    httpRequest.addQueryParam("version_type", request.versionType().name().toLowerCase());
                    break;
                case "INTERNAL":
                    // noop
                    break;
                default:
                    throw new IllegalStateException("version_type " + request.versionType() + " is not supported");
            }

            switch (request.consistencyLevel()) {
                case DEFAULT:
                    // noop
                    break;
                case ALL:
                case QUORUM:
                case ONE:
                    httpRequest.addQueryParam("consistency", request.consistencyLevel().name().toLowerCase());
                    break;
                default:
                    throw new IllegalStateException("consistency  " + request.consistencyLevel() + " is not supported");
            }
            switch (request.replicationType()) {
                case DEFAULT:
                    // noop
                    break;
                case SYNC:
                case ASYNC:
                    httpRequest.addQueryParam("replication", request.replicationType().name().toLowerCase());
                    break;
                default:
                    throw new IllegalStateException("replication  " + request.replicationType() + " is not supported");
            }

            if(request.refresh()) {
                httpRequest.addQueryParam("refresh", String.valueOf(true));
            }
            if(request.timeout() != ShardReplicationOperationRequest.DEFAULT_TIMEOUT) {
                httpRequest.addQueryParam("timeout", request.timeout().toString());
            }

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<DeleteResponse>(listener) {
                        @Override
                        protected DeleteResponse convert(Response response) {
                            return DeleteResponseParser.parse(response);
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
