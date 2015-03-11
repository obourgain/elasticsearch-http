package com.github.obourgain.elasticsearch.http.handler.search.suggest;

import static com.github.obourgain.elasticsearch.http.response.ErrorHandler.HANDLES_404;
import java.util.Map;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.suggest.SuggestAction;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestRequestAccessor;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.handler.search.clearscroll.ClearScrollResponse;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class SuggestActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(SuggestActionHandler.class);

    private final HttpClient httpClient;

    public SuggestActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public SuggestAction getAction() {
        return SuggestAction.INSTANCE;
    }

    public void execute(SuggestRequest request, final ActionListener<SuggestResponse> listener) {
        logger.debug("suggest request {}", request);
        try {
            // TODO test

            String indices = HttpRequestUtils.indicesOrAll(request);
            RequestUriBuilder uriBuilder = new RequestUriBuilder(indices, "_suggest");

            if (request.routing() != null) {
                // for Suggest requests, this can be a String[] but the SuggestRequests does the conversion to comma delimited string
                uriBuilder.addQueryParameter("routing", request.routing());
            }
            if (request.preference() != null) {
                uriBuilder.addQueryParameter("preference", request.preference());
            }

            uriBuilder.addIndicesOptions(request);

            HttpClientRequest<ByteBuf> httpRequest = HttpClientRequest.createGet(uriBuilder.toString());
            BytesReference source = SuggestRequestAccessor.getSource(request);
            if (source != null) {
                Tuple<XContentType, Map<String, Object>> queryAsMap = XContentHelper.convertToMap(source, false);
                Object version = queryAsMap.v2().get("version");
                if (version != null) {
                    if (version instanceof Boolean) {
                        uriBuilder.addQueryParameter("version", (Boolean) version);
                    } else {
                        logger.debug("version is not a boolean, got {}", version.getClass());
                    }
                }
                byte[] data = source.toBytes();
                httpRequest.withContent(data);
            }

            // TODO response
//            httpClient.client.submit(httpRequest)
//                    .flatMap(HANDLES_404)
//                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<ClearScrollResponse>>() {
//                        @Override
//                        public Observable<ClearScrollResponse> call(final HttpClientResponse<ByteBuf> response) {
//                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<ClearScrollResponse>>() {
//                                @Override
//                                public Observable<ClearScrollResponse> call(ByteBuf byteBuf) {
//                                    return ClearScrollResponse.parse(response.getStatus().code());
//                                }
//                            });
//                        }
//                    })
//                    .single()
//                    .subscribe(new ListenerCompleterObserver<>(listener));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
