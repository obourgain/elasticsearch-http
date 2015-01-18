package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestAccessor;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.admin.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.response.admin.indices.createindex.CreateIndexResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class CreateIndexActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(CreateIndexActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public CreateIndexActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public CreateIndexAction getAction() {
        return CreateIndexAction.INSTANCE;
    }

    public void execute(CreateIndexRequest request, final ActionListener<CreateIndexResponse> listener) {
        logger.debug("create index request {}", request);
        try {
            HttpClientImpl httpClient = indicesAdminClient.getHttpClient();
// TODO warmers
// TODO creation date http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-create-index.html#_creation_date

            String index = CreateIndexRequestAccessor.index(request);
            // TODO cause is unused in the rest API ?
            String cause = CreateIndexRequestAccessor.cause(request);

            Map<String, IndexMetaData.Custom> customs = CreateIndexRequestAccessor.customs(request);
            Map<String, String> mappings = CreateIndexRequestAccessor.mappings(request);
            Settings settings = CreateIndexRequestAccessor.settings(request);
            TimeValue timeout = CreateIndexRequestAccessor.timeout(request);
            TimeValue masterNodeTimeout = CreateIndexRequestAccessor.masterNodeTimeout(request);
            Set<Alias> aliases = CreateIndexRequestAccessor.aliases(request);

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePut(httpClient.getUrl() + "/" + index);

            // TODO inject, and this is quite dirty
            ObjectMapper objectMapper = new ObjectMapper();

            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder().startObject();
            if (!settings.getAsMap().isEmpty()) {
                jsonBuilder.field("settings").map(settings.getAsStructuredMap());
            }

            if (!mappings.isEmpty()) {
                jsonBuilder.field("mappings");
                for (Map.Entry<String, String> entry : mappings.entrySet()) {
                    Map mapping = objectMapper.readValue(entry.getValue(), Map.class);
                    // TODO maybe type is not always mandatory as key, depending on the content of the map
                    jsonBuilder.map(Collections.<String, Object>singletonMap(entry.getKey(), mapping));
                }
            }

            if (!aliases.isEmpty()) {
                jsonBuilder.field("aliases");
                jsonBuilder.startObject();
                for (Alias alias : aliases) {
                    jsonBuilder.startObject(alias.name());
                    String filter = alias.filter();
                    if (filter != null) {
                        jsonBuilder.startObject("filter");
                        jsonBuilder.field(filter);
                        jsonBuilder.endObject();
                    }
                    String searchRouting = alias.searchRouting();
                    if (searchRouting != null) {
                        jsonBuilder.field("search_routing", searchRouting);
                    }
                    String indexRouting = alias.indexRouting();
                    if (indexRouting != null) {
                        jsonBuilder.field("index_routing", indexRouting);
                    }
                    jsonBuilder.endObject();
                }
                jsonBuilder.endObject();
            }

            if (!customs.isEmpty()) {
                for (Map.Entry<String, IndexMetaData.Custom> entry : customs.entrySet()) {
                    IndexMetaData.Custom value = entry.getValue();
                    IndexMetaData.Custom.Factory<IndexMetaData.Custom> customFactory = IndexMetaData.lookupFactory(value.type());
                    if (customFactory == null) {
                        logger.warn("skipping unsupported custom field at index creation {} - {}", entry.getKey(), entry.getValue());
                    } else {
                        jsonBuilder.startObject(value.type());
                        customFactory.toXContent(value, jsonBuilder, new ToXContent.MapParams(Collections.<String, String>emptyMap()));
                        jsonBuilder.endObject();
                    }
                }
            }

            httpRequest.addQueryParam("timeout", timeout.toString());
            httpRequest.addQueryParam("master_timeout", masterNodeTimeout.toString());

            jsonBuilder.endObject();

            String body = jsonBuilder.string();
            httpRequest.setBody(body);

            httpRequest.execute(new ListenerAsyncCompletionHandler<CreateIndexResponse>(listener) {
                        @Override
                        protected CreateIndexResponse convert(Response response) {
                            return CreateIndexResponse.parse(response);
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
