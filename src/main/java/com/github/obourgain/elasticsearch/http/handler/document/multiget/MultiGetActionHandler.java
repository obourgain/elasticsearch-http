package com.github.obourgain.elasticsearch.http.handler.document.multiget;

import java.io.IOException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class MultiGetActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(MultiGetActionHandler.class);

    private final HttpClient httpClient;

    public MultiGetActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public GetAction getAction() {
        return GetAction.INSTANCE;
    }

    public void execute(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        logger.debug("multi get request {}", request);
        try {
            RequestUriBuilder uriBuilder = new RequestUriBuilder().addEndpoint("_mget");

            uriBuilder.addQueryParameter("ignore_errors_on_generated_fields", request.ignoreErrorsOnGeneratedFields);
            uriBuilder.addQueryParameterIfNotNull("preference", request.preference());
            uriBuilder.addQueryParameterIfNotNull("refresh", request.refresh());
            uriBuilder.addQueryParameterIfNotNull("realtime", request.realtime());

            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject().field("docs").startArray();
            for (MultiGetRequest.Item item : request.getItems()) {
                writeItem(builder, item);
            }
            builder.endArray().endObject();

            httpClient.client.submit(HttpClientRequest.createPost(uriBuilder.toString())
            .withContent(builder.bytes().toBytes()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<MultiGetResponse>>() {
                        @Override
                        public Observable<MultiGetResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<MultiGetResponse>>() {
                                @Override
                                public Observable<MultiGetResponse> call(ByteBuf byteBuf) {
                                    return MultiGetResponse.parse(byteBuf);
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

    private void writeItem(XContentBuilder builder, MultiGetRequest.Item item) throws IOException {
        builder.startObject();
        builder.field("_index", item.index());
        builder.field("_type", item.type());
        builder.field("_id", item.id());
        FetchSourceContext fetchSourceContext = item.fetchSourceContext();
        if(fetchSourceContext != null) {
            if(fetchSourceContext.fetchSource()) {
                if(fetchSourceContext.includes().length != 0 | fetchSourceContext.excludes().length != 0) {
                    builder.startObject("_source");
                    if(fetchSourceContext.includes().length != 0) {
                        builder.array("include", fetchSourceContext.includes());
                    }
                    if(fetchSourceContext.excludes().length != 0) {
                        builder.array("exclude", fetchSourceContext.excludes());

                    }
                    builder.endObject();
                }
            } else {
                builder.field("_source", false);
            }
        }
        if(item.fields() != null && item.fields().length != 0) {
            builder.array("fields", item.fields());
        }
        if(item.routing() != null) {
            builder.array("_routing", item.routing());
        }

        if(item.indicesOptions() != null) {
            builder.array("_routing", item.routing());
        }
        if(item.versionType() != VersionType.INTERNAL) {
            builder.array("_version_type", item.versionType().name().toLowerCase());
        }
        if(item.version() != Versions.MATCH_ANY) {
            builder.array("_version", item.version());
        }

        builder.endObject();
    }
}
