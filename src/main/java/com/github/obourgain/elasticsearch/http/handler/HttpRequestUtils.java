package com.github.obourgain.elasticsearch.http.handler;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import com.ning.http.client.AsyncHttpClient;

public class HttpRequestUtils {

    public static String indicesOrAll(IndicesRequest request) {
        String[] indices = request.indices();
        if(indices == null || indices.length == 0) {
            return "_all";
        }
        return Strings.arrayToCommaDelimitedString(indices);
    }

    public static void addIndicesOptions(AsyncHttpClient.BoundRequestBuilder httpRequest, IndicesRequest request) {
        IndicesOptions indicesOptions = request.indicesOptions();
        addBooleanParam(httpRequest, "ignore_unavailable", indicesOptions.ignoreUnavailable());
        addBooleanParam(httpRequest, "allow_no_indices", indicesOptions.allowNoIndices());

        if(indicesOptions.expandWildcardsOpen()) {
            httpRequest.addQueryParam("expand_wildcards", "open");
        }
        if(indicesOptions.expandWildcardsClosed()) {
            httpRequest.addQueryParam("expand_wildcards", "closed");
        }
        // TODO how are those set ?
        indicesOptions.allowAliasesToMultipleIndices();
        indicesOptions.forbidClosedIndices();
    }

    private static void addBooleanParam(AsyncHttpClient.BoundRequestBuilder httpRequest, String name, boolean value) {
        httpRequest.addQueryParam(name, String.valueOf(value));
    }

}
