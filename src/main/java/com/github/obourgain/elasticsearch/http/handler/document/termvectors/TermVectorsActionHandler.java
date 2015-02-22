package com.github.obourgain.elasticsearch.http.handler.document.termvectors;

import java.net.URLEncoder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.termvector.TermVectorAction;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class TermVectorsActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(TermVectorsActionHandler.class);

    private final HttpClient httpClient;

    public TermVectorsActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public TermVectorAction getAction() {
        return TermVectorAction.INSTANCE;
    }

    public void execute(TermVectorRequest request, final ActionListener<TermVectorResponse> listener) {
        logger.debug("term vector request {}", request);
        try {
            RequestUriBuilder uriBuilder = new RequestUriBuilder(request.index(), request.type(), URLEncoder.encode(request.id(), Charsets.UTF_8.displayName()))
                    .addEndpoint("_termvector");

            // TODO test
            uriBuilder.addQueryParameterIfNotNull("routing", request.routing());
            uriBuilder.addQueryParameterIfNotNull("preference", request.preference());

            uriBuilder.addQueryParameterCollectionAsCommaDelimitedIfNotNullNorEmpty("fields", request.selectedFields());

            uriBuilder.addQueryParameter("offsets", request.offsets());
            uriBuilder.addQueryParameter("positions", request.positions());
            uriBuilder.addQueryParameter("payloads", request.payloads());
            uriBuilder.addQueryParameter("term_statistics", request.termStatistics());
            uriBuilder.addQueryParameter("field_statistics", request.fieldStatistics());

            httpClient.client.submit(HttpClientRequest.createGet(uriBuilder.toString()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<TermVectorResponse>>() {
                        @Override
                        public Observable<TermVectorResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<TermVectorResponse>>() {
                                @Override
                                public Observable<TermVectorResponse> call(ByteBuf byteBuf) {
                                    return TermVectorResponse.parse(byteBuf);
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
