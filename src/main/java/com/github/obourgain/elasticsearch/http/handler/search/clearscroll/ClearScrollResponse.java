package com.github.obourgain.elasticsearch.http.handler.search.clearscroll;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import rx.Observable;

@Getter
@RequiredArgsConstructor
public class ClearScrollResponse {

    private final boolean succeeded;

    public static Observable<ClearScrollResponse> parse(int status) {
        boolean succeeded = status == 200;
        ClearScrollResponse result = new ClearScrollResponse(succeeded);
        return Observable.just(result);
    }

}
