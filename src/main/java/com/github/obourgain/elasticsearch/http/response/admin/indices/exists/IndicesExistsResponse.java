package com.github.obourgain.elasticsearch.http.response.admin.indices.exists;

import java.io.IOException;
import com.ning.http.client.Response;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class IndicesExistsResponse {

    private boolean exists;

    public static IndicesExistsResponse parse(Response response) {
        try {
            return doParse(response.getResponseBodyAsBytes(), response.getStatusCode());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static IndicesExistsResponse doParse(byte[] body, int status) {
        // TODO merge and handle correctly status
        switch (status) {
            case 200:
                return new IndicesExistsResponse(true);
            case 404:
                return new IndicesExistsResponse(false);
            // TODO when cluster blocks, I get a 403
            default:
                throw new IllegalStateException("status code " + status + " is not supported for indices exists request");
        }
    }
}
