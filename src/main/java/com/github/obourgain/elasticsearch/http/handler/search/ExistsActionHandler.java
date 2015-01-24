package com.github.obourgain.elasticsearch.http.handler.search;

import static com.github.obourgain.elasticsearch.http.request.HttpRequestUtils.addIndicesOptions;
import static com.github.obourgain.elasticsearch.http.request.HttpRequestUtils.indicesOrAll;
import static com.github.obourgain.elasticsearch.http.response.ValidStatusCodes._404;
import java.util.Set;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.exists.ExistsAction;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.exists.ExistsRequestAccessor;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.response.document.exists.ExistsResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class ExistsActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ExistsActionHandler.class);

    private final HttpClient httpClient;

    public ExistsActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public ExistsAction getAction() {
        return ExistsAction.INSTANCE;
    }

    public void execute(ExistsRequest request, final ActionListener<ExistsResponse> listener) {
        logger.debug("Exists request {}", request);
        try {
            // TODO test
            StringBuilder url = new StringBuilder(httpClient.getUrl()).append("/");

            String indices = indicesOrAll(request);
            url.append(indices);

            if (request.types() != null && request.types().length > 0) {
                url.append("/").append(Strings.arrayToCommaDelimitedString(request.types()));
            }
            url.append("/_search/exists");

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(url.toString());

            if (request.routing() != null) {
                httpRequest.addQueryParam("routing", request.routing());
            }
            if (request.preference() != null) {
                httpRequest.addQueryParam("preference", request.preference());
            }

            float minScore = ExistsRequestAccessor.minScore(request);
            httpRequest.addQueryParam("min_score", String.valueOf(minScore));

            addIndicesOptions(httpRequest, request);

            // TODO maybe I can use this with the standard ES deserializtion mecanism
            // request.source().streamInput();

            // TODO avoid doing to map conversion just to get one field
            BytesReference source = ExistsRequestAccessor.source(request);
            if (source != null) {
                httpRequest.setBody(source.toBytes());
            }

            // this will make ES return the _version field for each hit, which I need to build the ExistsHits object correctly
            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<ExistsResponse>(listener) {
                        @Override
                        protected ExistsResponse convert(Response response) {
                            return ExistsResponse.parse(response);
                        }

                        @Override
                        protected Set<Integer> non200ValidStatuses() {
                            return _404;
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
