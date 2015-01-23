package com.github.obourgain.elasticsearch.http.handler.search;

import static com.github.obourgain.elasticsearch.http.request.HttpRequestUtils.indicesOrAll;
import java.util.Map;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.suggest.SuggestAction;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestRequestAccessor;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.request.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class SuggestActionHandler implements ActionHandler<SuggestRequest, SuggestResponse, SuggestRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(SuggestActionHandler.class);

    private final HttpClient httpClient;

    public SuggestActionHandler(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public SuggestAction getAction() {
        return SuggestAction.INSTANCE;
    }

    @Override
    public void execute(SuggestRequest request, final ActionListener<SuggestResponse> listener) {
        logger.debug("suggest request {}", request);
        try {
            // TODO test
            StringBuilder url = new StringBuilder(httpClient.getUrl()).append("/");

            String indices = indicesOrAll(request);
            url.append(indices);
            url.append("/_suggest");

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(url.toString());

            if (request.routing() != null) {
                // for Suggest requests, this can be a String[] but the SuggestRequests does the conversion to comma delimited string
                httpRequest.addQueryParam("routing", request.routing());
            }
            if (request.preference() != null) {
                httpRequest.addQueryParam("preference", request.preference());
            }

            HttpRequestUtils.addIndicesOptions(httpRequest, request);

            BytesReference source = SuggestRequestAccessor.getSource(request);
            if(source != null) {
                Tuple<XContentType, Map<String, Object>> queryAsMap = XContentHelper.convertToMap(source, false);
                Object version = queryAsMap.v2().get("version");
                if (version != null) {
                    if (version instanceof Boolean) {
                        httpRequest.addQueryParam("version", String.valueOf(version));
                    } else {
                        logger.debug("version is not a boolean, got {}", version.getClass());
                    }
                }
                String data = XContentHelper.convertToJson(source, false);
                httpRequest.setBody(data);
            }

            httpRequest
                    .execute(new ListenerAsyncCompletionHandler<SuggestResponse>(request, listener) {
                        @Override
                        protected SuggestResponse convert(ResponseWrapper responseWrapper) {
                            return responseWrapper.toSuggestResponse();
                        }
                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
