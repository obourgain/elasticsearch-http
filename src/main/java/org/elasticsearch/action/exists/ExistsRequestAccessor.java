package org.elasticsearch.action.exists;

import org.elasticsearch.common.bytes.BytesReference;

public class ExistsRequestAccessor {

    public static float minScore(ExistsRequest request) {
        return request.minScore();
    }

    public static BytesReference source(ExistsRequest request) {
        return request.source();
    }

}
