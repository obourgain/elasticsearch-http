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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.admin.indices.template.put.PutIndexTemplateResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class PutIndexTemplateActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(PutIndexTemplateActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public PutIndexTemplateActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public PutIndexTemplateAction getAction() {
        return PutIndexTemplateAction.INSTANCE;
    }

    public void execute(PutIndexTemplateRequest request, final ActionListener<PutIndexTemplateResponse> listener) {
        // TODO test
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

            RequestUriBuilder uriBuilder = new RequestUriBuilder().addEndpoint("_template/" + request.name());

            // TODO inject, and this is quite dirty
            ObjectMapper objectMapper = new ObjectMapper();

            // TODO no way to nest XContentBuilders :'(
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().field("template", request.template());
            if (!mappings.isEmpty()) {
                xContentBuilder.field("mappings");
            }
            for (Map.Entry<String, String> entry : mappings.entrySet()) {
                Map mapping = objectMapper.readValue(entry.getValue(), Map.class);
                xContentBuilder.map(mapping);
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
            byte[] data = xContentBuilder.bytes().toBytes();

            // TODO make params optional
            uriBuilder
                    .addQueryParameter("order", order)
                    .addQueryParameter("template", request.template())
                    .addQueryParameter("master_timeout", timeValue.toString())
                    .addQueryParameter("create", create)
                    .addQueryParameter("cause", cause);

            indicesAdminClient.getHttpClient().submit(HttpClientRequest.createPut(uriBuilder.toString())
                    .withContent(data))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<PutIndexTemplateResponse>>() {
                        @Override
                        public Observable<PutIndexTemplateResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<PutIndexTemplateResponse>>() {
                                @Override
                                public Observable<PutIndexTemplateResponse> call(ByteBuf byteBuf) {
                                    return PutIndexTemplateResponse.parse(byteBuf, response.getStatus().code());
                                }
                            });
                        }
                    })
                    .single()
                    .subscribe(new ListenerCompleterObserver<>(listener));

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
