package com.github.obourgain.elasticsearch.http;

import static org.assertj.core.api.Assertions.assertThat;
import rx.Observable;

public class ObservableAsserts {

    public static void assertHasSize(Observable observable, int expected) {
        assertThat(observable.count().toBlocking().single()).isEqualTo(expected);
    }

    public static <T> T takeNth(Observable<T> observable, int n) {
        return observable.skip(n).take(1).toBlocking().single();
    }

}
