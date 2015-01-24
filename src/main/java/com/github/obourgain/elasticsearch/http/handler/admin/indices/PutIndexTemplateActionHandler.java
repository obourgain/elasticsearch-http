package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestAccessor;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class PutIndexTemplateActionHandler implements ActionHandler<PutIndexTemplateRequest, PutIndexTemplateResponse, PutIndexTemplateRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(PutIndexTemplateActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public PutIndexTemplateActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    @Override
    public PutIndexTemplateAction getAction() {
        return PutIndexTemplateAction.INSTANCE;
    }

    @Override
    public void execute(PutIndexTemplateRequest request, final ActionListener<PutIndexTemplateResponse> listener) {
        logger.debug("put index template request {}", request);
        try {
            String cause = request.cause();
            Set<Alias> aliases = PutIndexTemplateRequestAccessor.aliases(request);
            boolean create = request.create();
            Map<String, IndexMetaData.Custom> customs = PutIndexTemplateRequestAccessor.customs(request);
            int order = request.order();
            // TODO settings !
            Settings settings = PutIndexTemplateRequestAccessor.settings(request);
            TimeValue timeValue = request.masterNodeTimeout();
            Map<String, String> mappings = PutIndexTemplateRequestAccessor.mappings(request);

            HttpClient httpClient = indicesAdminClient.getHttpClient();
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.preparePut(httpClient.getUrl() + "/_template/" + request.name());

            // TODO inject, and this is quite dirty
//            ObjectMapper objectMapper = new ObjectMapper();

            // TODO no way to nest XContentBuilders :'(
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().field("template", request.template());
            if (!mappings.isEmpty()) {
                xContentBuilder.field("mappings");
            }
            for (Map.Entry<String, String> entry : mappings.entrySet()) {
//                Map mapping = objectMapper.readValue(entry.getValue(), Map.class);
//                xContentBuilder.map(mapping);
            }
            xContentBuilder.endObject();
            if (!aliases.isEmpty()) {
                xContentBuilder.field("aliases", aliases);
            }

            if (!customs.isEmpty()) {
                List<IndexMetaData.Custom> warmers = new ArrayList<>();
                for (Map.Entry<String, IndexMetaData.Custom> entry : customs.entrySet()) {
                    IndexMetaData.Custom value = entry.getValue();
                    switch (value.type()) {
                        case IndexWarmersMetaData.TYPE:
                            warmers.add(value);
                            break;
                        default:
                            logger.warn("custom type {} is not supported, please report the issue", value.type());
                    }
                }
                if (!warmers.isEmpty()) {
                    xContentBuilder.field("warmers", warmers);
                }
            }
            String data = xContentBuilder.string();

            // TODO make params optional
            httpRequest
                    .addQueryParam("order", String.valueOf(order))
                    .addQueryParam("template", String.valueOf(order))
                    .addQueryParam("master_timeout", String.valueOf(timeValue))
                    .addQueryParam("create", String.valueOf(create))
                    .addQueryParam("cause", cause)
                    .setBody(data)
                    .execute(new ListenerAsyncCompletionHandler<PutIndexTemplateResponse>(listener) {
                        @Override
                        protected PutIndexTemplateResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toPutIndexTemplateResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
