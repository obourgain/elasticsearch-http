package com.github.obourgain.elasticsearch.http.request;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.Strings;

public class HttpRequestUtils {

    public static String indicesOrAll(IndicesRequest request) {
        String[] indices = request.indices();
        if (indices == null || indices.length == 0) {
            return "_all";
        }
        return Strings.arrayToCommaDelimitedString(indices);
    }

}
