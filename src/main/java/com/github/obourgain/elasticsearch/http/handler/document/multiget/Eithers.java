package com.github.obourgain.elasticsearch.http.handler.document.multiget;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Eithers<L, R> implements Iterable<Either<L, R>> {

    private List<Either<L, R>> eithers = new ArrayList<>();

    public List<L> lefts() {
        List<L> result = new ArrayList<>();
        for (Either<L, R> either : eithers) {
            if(either.isLeft()) {
                result.add(either.left());
            }
        }
        return result;
    }

    public List<R> rights() {
        List<R> result = new ArrayList<>();
        for (Either<L, R> either : eithers) {
            if(either.isRight()) {
                result.add(either.right());
            }
        }
        return result;
    }

    @Override
    public Iterator<Either<L, R>> iterator() {
        return eithers.iterator();
    }

    public void add(Either<L, R> either) {
        eithers.add(either);
    }
}
