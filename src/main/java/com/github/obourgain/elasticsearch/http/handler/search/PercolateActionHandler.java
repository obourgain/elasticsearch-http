package com.github.obourgain.elasticsearch.http.handler.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.percolate.PercolateAction;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.handler.HttpRequestUtils;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.ning.http.client.AsyncHttpClient;

/**
 * @author olivier bourgain
 */
public class PercolateActionHandler implements ActionHandler<PercolateRequest, PercolateResponse, PercolateRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(PercolateActionHandler.class);

    private final HttpClientImpl httpClient;

    public PercolateActionHandler(HttpClientImpl httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public PercolateAction getAction() {
        return PercolateAction.INSTANCE;
    }

    @Override
    public void execute(PercolateRequest request, final ActionListener<PercolateResponse> listener) {
        // TODO test
        logger.debug("percolate request {}", request);
        GetRequest getRequest = request.getRequest();
        try {
            StringBuilder url = new StringBuilder(httpClient.getUrl()).append("/");
            if (getRequest != null) {
                url.append(getRequest.index())
                        .append("/")
                        .append(getRequest.type());
                if (getRequest.id() != null) {
                    url.append("/").append(getRequest.id());
                }
            } else {
                url.append(HttpRequestUtils.indicesOrAll(request))
                        .append("/")
                        .append(request.documentType());
            }


            url.append("/_percolate");

            if (request.onlyCount()) {
                url.append("/count");
            }

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(url.toString());

            if (getRequest != null) {
                if (getRequest.routing() != null) {
                    httpRequest.addQueryParam("routing", getRequest.routing());
                }
                if (getRequest.preference() != null) {
                    httpRequest.addQueryParam("preference", getRequest.preference());
                }
                if (getRequest.version() != Versions.MATCH_ANY) {
                    httpRequest.addQueryParam("version", String.valueOf(getRequest.version()));
                }
                // percolating an existing doc
                if (request.routing() != null) {
                    httpRequest.addQueryParam("percolate_routing", request.routing());
                }
                if (request.preference() != null) {
                    httpRequest.addQueryParam("percolate_preference", request.preference());
                }
                if (request.indices() != null) {
                    httpRequest.addQueryParam("percolate_index", Strings.arrayToCommaDelimitedString(request.indices()));
                }
                if (request.documentType() != null) {
                    httpRequest.addQueryParam("percolate_type", request.documentType());
                }

            } else {
                // params does not have the same meaning in percolate_doc and percolate_existing_doc
                if (request.routing() != null) {
                    httpRequest.addQueryParam("routing", request.routing());
                }
                if (request.preference() != null) {
                    httpRequest.addQueryParam("preference", request.preference());
                }
                if (request.documentType() != null) {
                    httpRequest.addQueryParam("type", request.documentType());
                }
            }

            if(request.source() != null) {
                String data = XContentHelper.convertToJson(request.source(), false);
                httpRequest.setBody(data);
            }
            HttpRequestUtils.addIndicesOptions(httpRequest, request);

            httpRequest.execute(new ListenerAsyncCompletionHandler<PercolateResponse>(listener) {
                @Override
                protected PercolateResponse convert(ResponseWrapper responseWrapper) {
                    return responseWrapper.toPercolateResponse();
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
