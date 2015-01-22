package com.github.obourgain.elasticsearch.http.handler.document;

import static com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils.addIndicesOptions;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.response.document.get.GetResponse;
import com.github.obourgain.elasticsearch.http.response.document.get.GetResponseParser;
import com.google.common.base.Charsets;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import io.reactivex.netty.RxNetty;

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
            String url = httpClient.getUrl() + "/" + request.index() + "/" + request.type() + "/" + URLEncoder.encode(request.id(), Charsets.UTF_8.displayName());
            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(url);

            addIndicesOptions(httpRequest, request);

            FetchSourceContext fetchSourceContext = request.fetchSourceContext();
            if(fetchSourceContext != null) {
                httpRequest.addQueryParam("_source", String.valueOf(fetchSourceContext.fetchSource()));
                if(fetchSourceContext.transformSource()) {
                    httpRequest.addQueryParam("_source_transform", String.valueOf(true));
                }
                // excludes & includes defaults to empty String array
                if(fetchSourceContext.excludes().length > 0) {
                    httpRequest.addQueryParam("_source_exclude", Strings.arrayToCommaDelimitedString(fetchSourceContext.excludes()));
                }
                if(fetchSourceContext.includes().length > 0) {
                    httpRequest.addQueryParam("_source_include", Strings.arrayToCommaDelimitedString(fetchSourceContext.includes()));
                }
            }

            if (request.version() != Versions.MATCH_ANY) {
                httpRequest.addQueryParam("version", String.valueOf(request.version()));
                httpRequest.addQueryParam("version_type", request.versionType().toString().toLowerCase());
            }
            if(request.fields() != null) {
                httpRequest.addQueryParam("fields", Strings.arrayToCommaDelimitedString(request.fields()));
            }
            if(request.routing() != null) {
                httpRequest.addQueryParam("routing", request.routing());
            }
            if(request.preference() != null) {
                httpRequest.addQueryParam("preference", request.preference());
            }
            if(request.refresh()) {
                httpRequest.addQueryParam("refresh", String.valueOf(request.refresh()));
            }
            if(request.realtime()) {
                httpRequest.addQueryParam("realtime", String.valueOf(request.realtime()));
            }

            RxNetty.createHttpGet(url)
                    .flatMap(GetResponseParser::parse)
                    .toBlocking().forEach(System.out::println);

//            httpRequest
//                    .setBody(request.toString())
//                    .execute(new ListenerAsyncCompletionHandler<GetResponse>(listener) {
//                        @Override
//                        protected GetResponse convert(Response response) {
//                            return GetResponseParser.parse(response);
//                        }
//                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
