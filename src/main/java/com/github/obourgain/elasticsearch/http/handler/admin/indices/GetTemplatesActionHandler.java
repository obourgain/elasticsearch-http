package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.admin.indices.template.get.GetIndexTemplatesResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class GetTemplatesActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GetTemplatesActionHandler.class);

    private final HttpIndicesAdminClient indicesAdminClient;

    public GetTemplatesActionHandler(HttpIndicesAdminClient indicesAdminClient) {
        this.indicesAdminClient = indicesAdminClient;
    }

    public GetIndexTemplatesAction getAction() {
        return GetIndexTemplatesAction.INSTANCE;
    }

    public void execute(GetIndexTemplatesRequest request, final ActionListener<GetIndexTemplatesResponse> listener) {
        logger.debug("get index templates request {}", request);
        try {
            // TODO test
            String names = Strings.arrayToCommaDelimitedString(request.names());
            RequestUriBuilder uriBuilder = new RequestUriBuilder().addEndpoint("_template/" + names);

            indicesAdminClient.getHttpClient().submit(HttpClientRequest.createGet(uriBuilder.toString()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<GetIndexTemplatesResponse>>() {
                        @Override
                        public Observable<GetIndexTemplatesResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<GetIndexTemplatesResponse>>() {
                                @Override
                                public Observable<GetIndexTemplatesResponse> call(ByteBuf byteBuf) {
                                    return GetIndexTemplatesResponse.parse(byteBuf);
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
