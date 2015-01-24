package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.google.common.collect.ImmutableMap;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class IndicesAliasesActionHandler implements ActionHandler<IndicesAliasesRequest, IndicesAliasesResponse, IndicesAliasesRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(IndicesAliasesActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public IndicesAliasesActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public IndicesAliasesAction getAction() {
        return IndicesAliasesAction.INSTANCE;
    }

    @Override
    public void execute(IndicesAliasesRequest request, final ActionListener<IndicesAliasesResponse> listener) {
        logger.debug("indices aliases request {}", request);
        try {
            HttpClient httpClient = indicesAdminClient.getHttpClient();

            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder().startObject();
            jsonBuilder.startArray("actions");

            List<IndicesAliasesRequest.AliasActions> actions = request.getAliasActions();
            List<Map<String, ? extends Map<String, Object>>> actionsAsMaps = new ArrayList<>();
            for (IndicesAliasesRequest.AliasActions action : actions) {
                for (String alias : action.aliases()) {
                    for (String index : action.indices()) {
                        aliasActionToJson(action, alias, index, jsonBuilder);
                    }
                }
            }
            jsonBuilder.endArray();

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePost(httpClient.getUrl() + "/_aliases");

            httpRequest.addQueryParam("timeout", request.timeout().toString());
            httpRequest.addQueryParam("master_timeout", request.masterNodeTimeout().toString());

            jsonBuilder.endObject();

            String body = jsonBuilder.string();
            httpRequest.setBody(body);
            HttpRequestUtils.addIndicesOptions(httpRequest, request);
            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<IndicesAliasesResponse>(listener) {
                        @Override
                        protected IndicesAliasesResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toIndicesAliasesResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }


    private void aliasActionToJson(IndicesAliasesRequest.AliasActions actions, String alias, String index, XContentBuilder builder) throws IOException {
        String filter = actions.aliasAction().filter();
        String indexRouting = actions.aliasAction().indexRouting();
        String searchRouting = actions.aliasAction().searchRouting();
        AliasAction.Type type = actions.aliasAction().actionType();

        builder.startObject();
        builder.startObject(type.name().toLowerCase());
        if(alias != null) {
            builder.field("alias", alias);
        }
        if(index != null) {
            builder.field("index", index);
        }
        if(filter != null) {
            builder.startObject("filter");
            builder.field(filter);
            builder.endObject();
//            XContentType xContentType = XContentFactory.xContentType(filter);
//            if (xContentType != null) {
//                try {
//                    Map<String, Object> filterAsMap = XContentFactory.xContent(xContentType).createParser(filter).mapAndClose();
//                    builder.field("filter", filterAsMap);
//                } catch (IOException e) {
//                    throw new ElasticsearchParseException("failed to parse filter for create alias", e);
//                }
//            }
        }
        if(searchRouting!= null) {
            builder.field("search_routing", searchRouting);
        }
        if(indexRouting != null) {
            builder.field("index_routing", indexRouting);
        }
        builder.endObject();
        builder.endObject();
    }

    private Map<String, ? extends Map<String, Object>> aliasActionToMap(IndicesAliasesRequest.AliasActions actions, String alias, String index) {
        String filter = actions.aliasAction().filter();
        String indexRouting = actions.aliasAction().indexRouting();
        String searchRouting = actions.aliasAction().searchRouting();
        AliasAction.Type type = actions.aliasAction().actionType();
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        if(alias != null) {
            builder.put("alias", alias);
        }
        if(index != null) {
            builder.put("index", index);
        }
        if(filter != null) {
            XContentType xContentType = XContentFactory.xContentType(filter);
            if (xContentType != null) {
                try {
                    Map<String, Object> filterAsMap = XContentFactory.xContent(xContentType).createParser(filter).mapAndClose();
                    builder.put("filter", filterAsMap);
                } catch (IOException e) {
                    throw new ElasticsearchParseException("failed to parse filter for create alias", e);
                }
            } else {
                builder.put("filter", filter);
            }
        }
        if(searchRouting!= null) {
            builder.put("search_routing", searchRouting);
        }
        if(indexRouting != null) {
            builder.put("index_routing", indexRouting);
        }
        return Collections.singletonMap(type.name().toLowerCase(), builder.build());
    }

}
