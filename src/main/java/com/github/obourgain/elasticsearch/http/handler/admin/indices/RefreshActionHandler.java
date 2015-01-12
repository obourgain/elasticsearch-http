package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import java.util.Map;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
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
public class RefreshActionHandler implements ActionHandler<RefreshRequest, RefreshResponse, RefreshRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(RefreshActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public RefreshActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public RefreshAction getAction() {
        return RefreshAction.INSTANCE;
    }

    @Override
    public void execute(RefreshRequest request, final ActionListener<RefreshResponse> listener) {
        logger.debug("refresh request {}", request);
        try {
            HttpClientImpl httpClient = indicesAdminClient.getHttpClient();

            String indices = HttpRequestUtils.indicesOrAll(request);

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(httpClient.getUrl() + "/" + indices + "/_refresh");

            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest.addQueryParam("force", String.valueOf(request.force()));
            httpRequest.execute(new ListenerAsyncCompletionHandler<RefreshResponse>(listener) {
                        @Override
                        protected RefreshResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toRefreshResponse();
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
