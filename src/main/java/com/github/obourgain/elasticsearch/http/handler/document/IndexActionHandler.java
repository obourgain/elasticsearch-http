package com.github.obourgain.elasticsearch.http.handler.document;

import java.net.URLEncoder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.response.index.IndexResponse;
import com.github.obourgain.elasticsearch.http.response.index.IndexResponseParser;
import com.google.common.base.Charsets;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class IndexActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(IndexActionHandler.class);

    private final HttpClient httpClient;

    public IndexActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public IndexAction getAction() {
        return IndexAction.INSTANCE;
    }

    public void execute(IndexRequest request, final ActionListener<IndexResponse> listener) {
        logger.debug("index request {}", request);
        try {
            String url = httpClient.getUrl() + "/" + request.index() + "/" + request.type();
            AsyncHttpClient.BoundRequestBuilder httpRequest;
            if(request.id() == null || request.id().length() == 0) {
                httpRequest = httpClient.asyncHttpClient.preparePost(url);
            } else {
                // encode to handle the case where the id got a space
                httpRequest = httpClient.asyncHttpClient.preparePut(url + "/" + URLEncoder.encode(request.id(), Charsets.UTF_8.displayName()));
            }
            if(request.version() != Versions.MATCH_ANY) {
                httpRequest.addQueryParam("version", String.valueOf(request.version()));
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
            if(request.opType() == IndexRequest.OpType.CREATE) {
                httpRequest.addQueryParam("op_type", "create");
            }
            if(request.routing() != null) {
                httpRequest.addQueryParam("routing", request.routing());
            }
            if(request.parent() != null) {
                httpRequest.addQueryParam("parent", request.parent());
            }
            if(request.timestamp() != null) {
                httpRequest.addQueryParam("timestamp", request.timestamp());
            }
            if(request.ttl() != -1) {
                httpRequest.addQueryParam("ttl", String.valueOf(request.ttl()));
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

            String requestBody = XContentHelper.convertToJson(request.source(), false);

            httpRequest
                    .setBody(requestBody)
                    .execute(new ListenerAsyncCompletionHandler<IndexResponse>(listener) {
                        @Override
                        protected IndexResponse convert(Response response) {
                            return IndexResponseParser.parse(response);
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
