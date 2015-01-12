package com.github.obourgain.elasticsearch.http.handler.document;

import java.io.IOException;
import java.net.URLEncoder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.response.update.UpdateResponse;
import com.github.obourgain.elasticsearch.http.response.update.UpdateResponseParser;
import com.google.common.base.Charsets;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class UpdateActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(UpdateActionHandler.class);

    private final HttpClientImpl httpClient;

    public UpdateActionHandler(HttpClientImpl httpClient) {
        this.httpClient = httpClient;
    }

    public UpdateAction getAction() {
        return UpdateAction.INSTANCE;
    }

    public void execute(UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        // TODO scripted_upsert
        logger.debug("update request {}", request);
        try {
            String url = httpClient.getUrl() + "/" + request.index() + "/" + request.type();
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(url + "/" + URLEncoder.encode(request.id(), Charsets.UTF_8.displayName()) + "/_update");

            buildRequest(request, httpRequest);

            httpRequest.setBody(UpdateHelper.buildRequestBody(request));
            httpRequest.execute(new ListenerAsyncCompletionHandler<UpdateResponse>(listener) {
                @Override
                protected UpdateResponse convert(Response response) {
                    return UpdateResponseParser.parse(response);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public static void buildRequest(UpdateRequest request, final AsyncHttpClient.BoundRequestBuilder httpRequest) throws IOException {
        if (request.version() != Versions.MATCH_ANY) {
            httpRequest.addQueryParam("version", String.valueOf(request.version()));
        }
        // do not use the enum's values because then this would depend on a specific version of ES and not allow a client with
        // an upper version of elasticsearch
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
        if (request.scriptLang() != null) {
            httpRequest.addQueryParam("lang", request.scriptLang());
        }
        if (request.routing() != null) {
            httpRequest.addQueryParam("routing", request.routing());
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
        if (request.fields() != null) {
            httpRequest.addQueryParam("fields", Strings.arrayToCommaDelimitedString(request.fields()));
        }
        if (request.refresh()) {
            httpRequest.addQueryParam("refresh", String.valueOf(true));
        }
        if (request.timeout() != ShardReplicationOperationRequest.DEFAULT_TIMEOUT) {
            httpRequest.addQueryParam("timeout", request.timeout().toString());
        }
        if (request.retryOnConflict() != 0) {
            httpRequest.addQueryParam("retry_on_conflict", String.valueOf(request.retryOnConflict()));
        }
    }

}
