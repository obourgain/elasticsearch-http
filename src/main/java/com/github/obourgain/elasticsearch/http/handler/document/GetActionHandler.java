package com.github.obourgain.elasticsearch.http.handler.document;

import java.net.URLEncoder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.github.obourgain.elasticsearch.http.response.document.get.GetResponse;
import com.github.obourgain.elasticsearch.http.response.document.get.GetResponseParser;
import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class GetActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(GetActionHandler.class);

    private final HttpClient httpClient;

    public GetActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public GetAction getAction() {
        return GetAction.INSTANCE;
    }

    public void execute(final GetRequest request, final ActionListener<GetResponse> listener) {
        logger.debug("get request {}", request);
        try {
            // encode to handle the case where the id got a space/special char
            RequestUriBuilder uriBuilder = new RequestUriBuilder(request.index(), request.type(), URLEncoder.encode(request.id(), Charsets.UTF_8.displayName()));
            uriBuilder.addIndicesOptions(request);

            FetchSourceContext fetchSourceContext = request.fetchSourceContext();
            if (fetchSourceContext != null) {
                uriBuilder.addQueryParameter("_source", fetchSourceContext.fetchSource());
                if (fetchSourceContext.transformSource()) {
                    uriBuilder.addQueryParameter("_source_transform", true);
                }
                // excludes & includes defaults to empty String array
                if (fetchSourceContext.excludes().length > 0) {
                    uriBuilder.addQueryParameterArrayAsCommaDelimited("_source_exclude", fetchSourceContext.excludes());
                }
                if (fetchSourceContext.includes().length > 0) {
                    uriBuilder.addQueryParameterArrayAsCommaDelimited("_source_include", fetchSourceContext.includes());
                }
            }

            if (request.version() != Versions.MATCH_ANY) {
                uriBuilder.addQueryParameter("version", request.version());
                uriBuilder.addQueryParameter("version_type", request.versionType().toString().toLowerCase());
            }
            if (request.fields() != null) {
                uriBuilder.addQueryParameterArrayAsCommaDelimited("fields", request.fields());
            }
            uriBuilder.addQueryParameterIfNotNull("routing", request.routing());
            uriBuilder.addQueryParameterIfNotNull("preference", request.preference());
            if (request.refresh()) {
                uriBuilder.addQueryParameter("refresh", request.refresh());
            }
            if (request.realtime()) {
                uriBuilder.addQueryParameter("realtime", request.realtime());
            }
            httpClient.client.submit(HttpClientRequest.createGet(uriBuilder.toString()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<GetResponse>>() {
                                @Override
                                public Observable<GetResponse> call(ByteBuf byteBuf) {
                                    return GetResponseParser.parse(byteBuf);
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
