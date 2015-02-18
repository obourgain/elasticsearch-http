package com.github.obourgain.elasticsearch.http.handler.document.multiget;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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
            RequestUriBuilder uriBuilder = uriBuilder = new RequestUriBuilder()
                    .addEndpoint("_mget");

            XContentBuilder builder = XContentFactory.jsonBuilder();

            uriBuilder.addQueryParameter("ignore_errors_on_generated_fields", request.ignoreErrorsOnGeneratedFields);
            uriBuilder.addQueryParameterIfNotNull("preference", request.preference());
            uriBuilder.addQueryParameterIfNotNull("refresh", request.refresh());
            uriBuilder.addQueryParameterIfNotNull("realtime", request.realtime());

//            request.subRequests();
//
//            request.getItems();
//
//                    FetchSourceContext fetchSourceContext = request.fetchSourceContext();
//            if (fetchSourceContext != null) {
//                uriBuilder.addQueryParameter("_source", fetchSourceContext.fetchSource());
//                if (fetchSourceContext.transformSource()) {
//                    uriBuilder.addQueryParameter("_source_transform", true);
//                }
//                // excludes & includes defaults to empty String array
//                if (fetchSourceContext.excludes().length > 0) {
//                    uriBuilder.addQueryParameterArrayAsCommaDelimited("_source_exclude", fetchSourceContext.excludes());
//                }
//                if (fetchSourceContext.includes().length > 0) {
//                    uriBuilder.addQueryParameterArrayAsCommaDelimited("_source_include", fetchSourceContext.includes());
//                }
//            }
//
//            if (request.version() != Versions.MATCH_ANY) {
//                uriBuilder.addQueryParameter("version", request.version());
//                uriBuilder.addQueryParameter("version_type", request.versionType().toString().toLowerCase());
//            }
//            if (request.fields() != null) {
//                uriBuilder.addQueryParameterArrayAsCommaDelimited("fields", request.fields());
//            }
//            uriBuilder.addQueryParameterIfNotNull("routing", request.routing());
//            uriBuilder.addQueryParameterIfNotNull("preference", request.preference());
//            if (request.refresh()) {
//                uriBuilder.addQueryParameter("refresh", request.refresh());
//            }
//            if (request.realtime()) {
//                uriBuilder.addQueryParameter("realtime", request.realtime());
//            }
            httpClient.client.submit(HttpClientRequest.createGet(uriBuilder.toString()))
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
}
