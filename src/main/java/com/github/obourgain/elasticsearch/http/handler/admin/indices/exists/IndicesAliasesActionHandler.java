package com.github.obourgain.elasticsearch.http.handler.admin.indices.exists;

import java.io.IOException;
import java.util.List;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.admin.indices.aliases.IndicesAliasesResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class IndicesAliasesActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(IndicesAliasesActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public IndicesAliasesActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public IndicesAliasesAction getAction() {
        return IndicesAliasesAction.INSTANCE;
    }

    public void execute(IndicesAliasesRequest request, final ActionListener<IndicesAliasesResponse> listener) {
        logger.debug("indices aliases request {}", request);
        try {
            RequestUriBuilder uriBuilder = new RequestUriBuilder().addEndpoint("_aliases");

            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder().startObject();
            jsonBuilder.startArray("actions");

            List<IndicesAliasesRequest.AliasActions> actions = request.getAliasActions();
            for (IndicesAliasesRequest.AliasActions action : actions) {
                for (String alias : action.aliases()) {
                    for (String index : action.indices()) {
                        aliasActionToJson(action, alias, index, jsonBuilder);
                    }
                }
            }
            jsonBuilder.endArray();
            jsonBuilder.endObject();

            uriBuilder.addQueryParameter("timeout", request.timeout().toString());
            uriBuilder.addQueryParameter("master_timeout", request.masterNodeTimeout().toString());

            uriBuilder.addIndicesOptions(request.indicesOptions());

            byte[] body = jsonBuilder.bytes().toBytes();

            indicesAdminClient.getHttpClient().submit(HttpClientRequest.createPost(uriBuilder.toString())
                    .withContent(body))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<IndicesAliasesResponse>>() {
                        @Override
                        public Observable<IndicesAliasesResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<IndicesAliasesResponse>>() {
                                @Override
                                public Observable<IndicesAliasesResponse> call(ByteBuf byteBuf) {
                                    return IndicesAliasesResponse.parse(byteBuf, response.getStatus().code());
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


    private void aliasActionToJson(IndicesAliasesRequest.AliasActions actions, String alias, String index, XContentBuilder builder) throws IOException {
        String filter = actions.aliasAction().filter();
        String indexRouting = actions.aliasAction().indexRouting();
        String searchRouting = actions.aliasAction().searchRouting();
        AliasAction.Type type = actions.aliasAction().actionType();

        builder.startObject();
        builder.startObject(type.name().toLowerCase());
        if (alias != null) {
            builder.field("alias", alias);
        }
        if (index != null) {
            builder.field("index", index);
        }
        if (filter != null) {
            builder.rawField("filter", filter.getBytes());
        }
        if (searchRouting != null) {
            builder.field("search_routing", searchRouting);
        }
        if (indexRouting != null) {
            builder.field("index_routing", indexRouting);
        }
        builder.endObject();
        builder.endObject();
    }

}
