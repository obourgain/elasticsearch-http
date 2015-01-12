package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import java.util.Map;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.optimize.OptimizeAction;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequestBuilder;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.admin.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.google.common.collect.ImmutableMap;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class OptimizeActionHandler implements ActionHandler<OptimizeRequest, OptimizeResponse, OptimizeRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(OptimizeActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public OptimizeActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public OptimizeAction getAction() {
        return OptimizeAction.INSTANCE;
    }

    @Override
    public void execute(OptimizeRequest request, final ActionListener<OptimizeResponse> listener) {
        logger.debug("optimize request {}", request);
        try {
            HttpClientImpl httpClient = indicesAdminClient.getHttpClient();

            // TODO wait if ongoing as param in more recent versions

            String indices = HttpRequestUtils.indicesOrAll(request);

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(httpClient.getUrl() + "/" + indices + "/_optimize");
            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest.addQueryParam("force", String.valueOf(request.force()));
            httpRequest.addQueryParam("flush", String.valueOf(request.flush()));
            httpRequest.addQueryParam("max_num_segments", String.valueOf(request.maxNumSegments()));
            httpRequest.addQueryParam("only_expunge_deletes", String.valueOf(request.onlyExpungeDeletes()));
            httpRequest.addQueryParam("wait_for_merge", String.valueOf(request.waitForMerge()));

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<OptimizeResponse>(listener) {
                        @Override
                        protected OptimizeResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toOptimizeResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private Map<String, Object> aliasToMap(Alias alias, String index) {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("alias", alias.name());
        builder.put("index", index);
        String filter = alias.filter();
        if (filter != null) {
            builder.put("filter", filter);
        }
        String searchRouting = alias.searchRouting();
        if (searchRouting != null) {
            builder.put("search_routing", searchRouting);
        }
        String indexRouting = alias.indexRouting();
        if (indexRouting != null) {
            builder.put("index_routing", indexRouting);
        }
        return builder.build();
    }

}
