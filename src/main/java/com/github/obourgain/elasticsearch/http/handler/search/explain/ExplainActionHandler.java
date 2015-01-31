package com.github.obourgain.elasticsearch.http.handler.search.explain;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.explain.ExplainAction;
import org.elasticsearch.action.explain.ExplainRequest;
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
public class ExplainActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ExplainActionHandler.class);

    private final HttpClient httpClient;

    public ExplainActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public ExplainAction getAction() {
        return ExplainAction.INSTANCE;
    }

    public void execute(ExplainRequest request, final ActionListener<ExplainResponse> listener) {
        logger.debug("explain request {}", request);
        try {
            RequestUriBuilder uriBuilder = new RequestUriBuilder(request.index(), request.type(), request.id())
                    .addEndpoint("/_explain");

            if (request.fetchSourceContext() != null) {
                if (request.fetchSourceContext().fetchSource()) {
                    uriBuilder.addQueryParameter("_source", request.fetchSourceContext().fetchSource());
                }
                // excludes & includes defaults to empty String array
                if (request.fetchSourceContext().excludes().length > 0) {
                    uriBuilder.addQueryParameter("_source_exclude", request.fetchSourceContext().excludes());
                }
                if (request.fetchSourceContext().includes().length > 0) {
                    uriBuilder.addQueryParameter("_source_include", request.fetchSourceContext().excludes());
                }
            }

            uriBuilder.addQueryParameterArrayAsCommaDelimitedIfNotNullNorEmpty("fields", request.fields());
            uriBuilder.addQueryParameterIfNotNull("preference", request.preference());

            // for search requests, this can be a String[] but the SearchRequests does the conversion to comma delimited string
            uriBuilder.addQueryParameterIfNotNull("routing", request.routing());

            HttpClientRequest<ByteBuf> httpRequest = HttpClientRequest.createGet(uriBuilder.toString())
                    .withContent(request.source().toBytes());

            httpClient.client.submit(httpRequest)
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<ExplainResponse>>() {
                        @Override
                        public Observable<ExplainResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<ExplainResponse>>() {
                                @Override
                                public Observable<ExplainResponse> call(ByteBuf byteBuf) {
                                    return ExplainResponse.parse(byteBuf);
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
