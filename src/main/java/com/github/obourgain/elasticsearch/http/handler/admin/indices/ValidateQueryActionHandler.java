package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateRequestAccessor;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpIndicesAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.validate.ValidateQueryResponse;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class ValidateQueryActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ValidateQueryActionHandler.class);

    private final HttpIndicesAdminClient httpClient;

    public ValidateQueryActionHandler(HttpIndicesAdminClient httpClient) {
        this.httpClient = httpClient;
    }

    public ValidateQueryAction getAction() {
        return ValidateQueryAction.INSTANCE;
    }

    public void execute(ValidateQueryRequest request, final ActionListener<ValidateQueryResponse> listener) {
        // TODO test
        logger.debug("validate query request {}", request);
        try {
            RequestUriBuilder uriBuilder;
            if (request.types() != null) {
                uriBuilder = new RequestUriBuilder(Strings.arrayToCommaDelimitedString(request.indices()), Strings.arrayToCommaDelimitedString(request.types()));
            } else {
                uriBuilder = new RequestUriBuilder(Strings.arrayToCommaDelimitedString(request.indices()));
            }
            uriBuilder.addEndpoint("/_validate/query")
                    .addIndicesOptions(request);

            if (request.explain()) {
                uriBuilder.addQueryParameter("explain", "true");
            }

            HttpClientRequest<ByteBuf> httpRequest = HttpClientRequest.createPost(uriBuilder.toString());

            if(ValidateRequestAccessor.getSource(request) != null) {
                httpRequest.withContent(ValidateRequestAccessor.getSource(request).toBytes());
            }

            httpClient.getHttpClient().client.submit(httpRequest)
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<ValidateQueryResponse>>() {
                        @Override
                        public Observable<ValidateQueryResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<ValidateQueryResponse>>() {
                                @Override
                                public Observable<ValidateQueryResponse> call(ByteBuf byteBuf) {
                                    return ValidateQueryResponse.parse(byteBuf);
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
