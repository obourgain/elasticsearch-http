package com.github.obourgain.elasticsearch.http.handler.search.suggest;

import static com.github.obourgain.elasticsearch.http.response.ErrorHandler.AS_FUNC;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.suggest.SuggestAction;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestRequestAccessor;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
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
            String indices = HttpRequestUtils.indicesOrAll(request);
            RequestUriBuilder uriBuilder = new RequestUriBuilder(indices, "_suggest");

            // for Suggest requests, this can be a String[] but the SuggestRequests does the conversion to comma delimited string
            uriBuilder.addQueryParameterIfNotNull("routing", request.routing());
            uriBuilder.addQueryParameterIfNotNull("preference", request.preference());

            uriBuilder.addIndicesOptions(request);

            HttpClientRequest<ByteBuf> httpRequest = HttpClientRequest.createGet(uriBuilder.toString());
            BytesReference source = SuggestRequestAccessor.getSource(request);
            if (source != null) {
                XContentParser parser = XContentHelper.createParser(source);
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject()
                        .copyCurrentStructure(parser)
                        .endObject();
                httpRequest.withContent(builder.bytes().toBytes());
            }

            httpClient.getHttpClient().submit(httpRequest)
                    .flatMap(AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<SuggestResponse>>() {
                        @Override
                        public Observable<SuggestResponse> call(final HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<SuggestResponse>>() {
                                @Override
                                public Observable<SuggestResponse> call(ByteBuf byteBuf) {
                                    return SuggestResponse.parse(byteBuf);
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
