package com.github.obourgain.elasticsearch.http.handler.search.percolate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.percolate.PercolateAction;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.common.lucene.uid.Versions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
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
public class PercolateActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(PercolateActionHandler.class);

    private final HttpClient httpClient;

    public PercolateActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public PercolateAction getAction() {
        return PercolateAction.INSTANCE;
    }

    public void execute(PercolateRequest request, final ActionListener<PercolateResponse> listener) {
        logger.debug("percolate request {}", request);
        // percolate_format does not exist in PercolateRequest, only in REST API
        RequestUriBuilder uriBuilder;

        GetRequest getRequest = request.getRequest();
        try {
            if (getRequest != null) {
                if (getRequest.id() != null) {
                    uriBuilder = new RequestUriBuilder(getRequest.index(), getRequest.type(), getRequest.id());
                } else {
                    uriBuilder = new RequestUriBuilder(getRequest.index(), getRequest.type());
                }
            } else {
                uriBuilder = new RequestUriBuilder(HttpRequestUtils.indicesOrAll(request), request.documentType());
            }

            if (request.onlyCount()) {
                uriBuilder.addEndpoint("/_percolate/count");
            } else {
                uriBuilder.addEndpoint("/_percolate");
            }

            if (getRequest != null) {
                uriBuilder.addQueryParameterIfNotNull("routing", getRequest.routing());
                uriBuilder.addQueryParameterIfNotNull("preference", getRequest.preference());
                if (getRequest.version() != Versions.MATCH_ANY) {
                    uriBuilder.addQueryParameter("version", getRequest.version());
                }
                // percolating an existing doc
                uriBuilder.addQueryParameterIfNotNull("percolate_routing", request.routing());
                uriBuilder.addQueryParameterIfNotNull("percolate_preference", request.preference());
                uriBuilder.addQueryParameterArrayAsCommaDelimitedIfNotNullNorEmpty("percolate_index", request.indices());
                uriBuilder.addQueryParameterIfNotNull("percolate_type", request.documentType());

            } else {
                // params does not have the same meaning in percolate_doc and percolate_existing_doc
                uriBuilder.addQueryParameterIfNotNull("routing", request.routing());
                uriBuilder.addQueryParameterIfNotNull("preference", request.preference());
                uriBuilder.addQueryParameterIfNotNull("type", request.documentType());
            }
            uriBuilder.addIndicesOptions(request);

            uriBuilder.addQueryParameter("pretty", true);
            HttpClientRequest<ByteBuf> httpRequest = HttpClientRequest.createGet(uriBuilder.toString());

            if (request.source() != null) {
                httpRequest.withContent(request.source().toBytes());
            }

            httpClient.getHttpClient().submit(httpRequest)
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<PercolateResponse>>() {
                        @Override
                        public Observable<PercolateResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<PercolateResponse>>() {
                                @Override
                                public Observable<PercolateResponse> call(ByteBuf byteBuf) {
                                    return PercolateResponse.parse(byteBuf);
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
