package com.github.obourgain.elasticsearch.http.response;

import java.io.IOException;
import org.elasticsearch.common.hppc.IntSet;
import com.ning.http.client.Response;

public class ErrorHandler {

    public static void checkError(Response response, IntSet non200ValidStatuses) {
        int statusCode = response.getStatusCode();
        if(statusCode > 300 && !non200ValidStatuses.contains(statusCode)) {
            try {
                throw new ElasticsearchHttpException(response.getResponseBody(), statusCode);
            } catch (IOException e) {
                RuntimeException exception = new RuntimeException("Unable to read response", e);
                exception.addSuppressed(new ElasticsearchHttpException(statusCode));
                throw exception;
            }
        }
    }

}
