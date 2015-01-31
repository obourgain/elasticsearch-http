package com.github.obourgain.elasticsearch.http.response.admin.indices.exists;

import lombok.AllArgsConstructor;
import lombok.Getter;
import rx.Observable;

@Getter
@AllArgsConstructor
public class IndicesExistsResponse {

    private boolean exists;

    public static Observable<IndicesExistsResponse> parse(int status) {
        return Observable.just(doParse(status));
    }

    protected static IndicesExistsResponse doParse(int status) {
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
