package com.github.obourgain.elasticsearch.http.handler.search.search;

import static org.elasticsearch.action.search.SearchType.DEFAULT;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
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
public class MultiSearchActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(MultiSearchActionHandler.class);
    public static final byte[] LINE_FEED = "\n".getBytes();

    private final HttpClient httpClient;

    public MultiSearchActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public SearchAction getAction() {
        return SearchAction.INSTANCE;
    }

    public void execute(MultiSearchRequest request, final ActionListener<MultiSearchResponse> listener) {
        logger.debug("multi search request {}", request);
        try {
            RequestUriBuilder uriBuilder = new RequestUriBuilder();
            uriBuilder.addEndpoint("_msearch");
            uriBuilder.addIndicesOptions(request.indicesOptions());

            // TODO convert lazily
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for (SearchRequest searchRequest : request.requests()) {
                writeHeader(searchRequest, outputStream);
                outputStream.write(LINE_FEED);
                outputStream.write(searchRequest.source().toBytes());
                outputStream.write(LINE_FEED);
            }

            httpClient.getHttpClient().submit(HttpClientRequest.createPost(uriBuilder.toString())
                    .withContent(outputStream.toByteArray()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<MultiSearchResponse>>() {
                        @Override
                        public Observable<MultiSearchResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<MultiSearchResponse>>() {
                                @Override
                                public Observable<MultiSearchResponse> call(ByteBuf byteBuf) {
                                    return MultiSearchResponse.parse(byteBuf);
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

    private void writeHeader(SearchRequest request, ByteArrayOutputStream outputStream) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder(outputStream)) {
            builder.startObject();
            builder.field("index", request.indices());
            if(request.types() != null && request.types().length != 0) {
                builder.field("type", request.types());
            }
            if(request.searchType() != DEFAULT) {
                builder.field("search_type", request.searchType().name().toLowerCase());
            }
            if(request.preference() != null) {
                builder.field("preference", request.preference());
            }
            if(request.routing() != null) {
                builder.field("routing", request.routing());
            }
            builder.endObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
