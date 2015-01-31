package com.github.obourgain.elasticsearch.http.handler.search.clearscroll;

import lombok.Getter;
import rx.Observable;

@Getter
public class ClearScrollResponse {

    private boolean succeeded;


    public static Observable<ClearScrollResponse> parse(int status) {
        ClearScrollResponse result = new ClearScrollResponse();
        result.succeeded = status == 200;
        return Observable.just(result);
    }

}
