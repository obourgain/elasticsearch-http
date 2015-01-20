package com.github.obourgain.elasticsearch.http.handler.document;

import static com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils.addIndicesOptions;
import static com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils.indicesOrAll;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestAccessor;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.response.document.deleteByQuery.DeleteByQueryResponse;
import com.github.obourgain.elasticsearch.http.response.document.deleteByQuery.DeleteByQueryResponseParser;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class DeleteByQueryActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(DeleteByQueryActionHandler.class);

    private final HttpClient httpClient;

    public DeleteByQueryActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public DeleteByQueryAction getAction() {
        return DeleteByQueryAction.INSTANCE;
    }

    public void execute(DeleteByQueryRequest request, final ActionListener<DeleteByQueryResponse> listener) {
        logger.debug("delete by query request {}", request);
        try {
            StringBuilder url = new StringBuilder(httpClient.getUrl()).append("/");

            String indices = indicesOrAll(request);
            url.append(indices);

            String[] types = DeleteByQueryRequestAccessor.types(request);
            if (types != null && types.length != 0) {
                url.append("/").append(Strings.arrayToCommaDelimitedString(types));
            }
            url.append("/_query");

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareDelete(url.toString());
            addIndicesOptions(httpRequest, request);

            if (request.routing() != null) {
                // for search requests, this can be a String[] but the SearchRequests does the conversion to comma delimited string
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
            if(request.timeout() != ShardReplicationOperationRequest.DEFAULT_TIMEOUT) {
                httpRequest.addQueryParam("timeout", request.timeout().toString());
            }

            String data = XContentHelper.convertToJson(DeleteByQueryRequestAccessor.getSource(request), false);
            httpRequest
                    .setBody(data)
                    .execute(new ListenerAsyncCompletionHandler<DeleteByQueryResponse>(listener) {
                        @Override
                        protected DeleteByQueryResponse convert(Response response) {
                            return DeleteByQueryResponseParser.parse(response);
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
