package com.github.obourgain.elasticsearch.http.response.clearscroll;

import com.ning.http.client.Response;
import lombok.Getter;

@Getter
public class ClearScrollResponse {

    private boolean succeeded;

    public static ClearScrollResponse parse(Response response) {
        int statusCode = response.getStatusCode();
        ClearScrollResponse result = new ClearScrollResponse();
        result.succeeded = statusCode == 200;
        return result;
    }
}
