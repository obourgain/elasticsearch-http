package com.github.obourgain.elasticsearch.http.handler.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.suggest.SuggestAction;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClient;

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
//            // TODO test
//            StringBuilder url = new StringBuilder(httpClient.getUrl()).append("/");
//
//            String indices = indicesOrAll(request);
//            url.append(indices);
//            url.append("/_suggest");
//
//            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(url.toString());
//
//            if (request.routing() != null) {
//                // for Suggest requests, this can be a String[] but the SuggestRequests does the conversion to comma delimited string
//                httpRequest.addQueryParam("routing", request.routing());
//            }
//            if (request.preference() != null) {
//                httpRequest.addQueryParam("preference", request.preference());
//            }
//
//            HttpRequestUtils.addIndicesOptions(httpRequest, request);
//
//            BytesReference source = SuggestRequestAccessor.getSource(request);
//            if (source != null) {
//                Tuple<XContentType, Map<String, Object>> queryAsMap = XContentHelper.convertToMap(source, false);
//                Object version = queryAsMap.v2().get("version");
//                if (version != null) {
//                    if (version instanceof Boolean) {
//                        httpRequest.addQueryParam("version", String.valueOf(version));
//                    } else {
//                        logger.debug("version is not a boolean, got {}", version.getClass());
//                    }
//                }
//                String data = XContentHelper.convertToJson(source, false);
//                httpRequest.setBody(data);
//            }
//
//            httpRequest
//                    .execute(new ListenerAsyncCompletionHandler<SuggestResponse>(request, listener) {
//                        @Override
//                        protected SuggestResponse convert(ResponseWrapper responseWrapper) {
//                            return responseWrapper.toSuggestResponse();
//                        }
//                    });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
