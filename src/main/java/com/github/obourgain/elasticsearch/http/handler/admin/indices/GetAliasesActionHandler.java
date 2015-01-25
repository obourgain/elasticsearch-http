package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.admin.indices.getaliases.GetAliasesResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class GetAliasesActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GetAliasesActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public GetAliasesActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public GetAliasesAction getAction() {
        return GetAliasesAction.INSTANCE;
    }

    public void execute(GetAliasesRequest request, final ActionListener<GetAliasesResponse> listener) {
        logger.debug("get aliases request {}", request);
        try {
            String indices = HttpRequestUtils.indicesOrAll(request);
            String aliases = Strings.arrayToCommaDelimitedString(request.aliases());
            if (!aliases.isEmpty()) {
                aliases = "/_alias/" + aliases;
            }
            RequestUriBuilder uriBuilder = new RequestUriBuilder(indices).addEndpoint(aliases);
            uriBuilder.addIndicesOptions(request);

            uriBuilder.addQueryParameter("master_timeout", request.masterNodeTimeout().toString());
            uriBuilder.addQueryParameter("local", request.local());

            indicesAdminClient.getHttpClient().client.submit(HttpClientRequest.createGet(uriBuilder.toString()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<GetAliasesResponse>>() {
                        @Override
                        public Observable<GetAliasesResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<GetAliasesResponse>>() {
                                @Override
                                public Observable<GetAliasesResponse> call(ByteBuf byteBuf) {
                                    return GetAliasesResponse.parse(byteBuf);
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
