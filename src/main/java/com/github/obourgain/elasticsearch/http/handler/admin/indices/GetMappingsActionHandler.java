package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import static com.github.obourgain.elasticsearch.http.response.ErrorHandler.*;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.ValidStatusCodes;
import com.github.obourgain.elasticsearch.http.response.admin.indices.mapping.get.GetMappingsResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class GetMappingsActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GetMappingsActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public GetMappingsActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public GetMappingsAction getAction() {
        return GetMappingsAction.INSTANCE;
    }

    public void execute(GetMappingsRequest request, final ActionListener<GetMappingsResponse> listener) {
        // TODO tests
        logger.debug("get mappings request {}", request);
        try {
            String indices = HttpRequestUtils.indicesOrAll(request);
            RequestUriBuilder uriBuilder = new RequestUriBuilder(indices);

            String types = Strings.arrayToCommaDelimitedString(request.types());
            if (!types.isEmpty()) {
                uriBuilder.type(types);
            }
            uriBuilder.addEndpoint("_mapping");
            // lots of url patterns are accepted, but this one is the most practical for a generic impl

            uriBuilder.addQueryParameter("master_timeout", request.masterNodeTimeout().toString());

            indicesAdminClient.getHttpClient().submit(HttpClientRequest.createGet(uriBuilder.toString()))
                    .flatMap(HANDLES_404)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<GetMappingsResponse>>() {
                        @Override
                        public Observable<GetMappingsResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<GetMappingsResponse>>() {
                                @Override
                                public Observable<GetMappingsResponse> call(ByteBuf byteBuf) {
                                    return GetMappingsResponse.parse(byteBuf);
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
