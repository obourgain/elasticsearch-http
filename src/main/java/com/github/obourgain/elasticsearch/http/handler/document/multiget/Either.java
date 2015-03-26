package com.github.obourgain.elasticsearch.http.handler.document.multiget;

import static java.util.Objects.requireNonNull;

public class Either<L, R> {

    private final L left;
    private final R right;

    public Either(L left, R right) {
        this.left = left;
        this.right = right;
    }

    public static <L, R> Either<L, R> left(L left) {
        return new Either<>(requireNonNull(left), null);
    }

    public static <L, R> Either<L, R> right(R right) {
        return new Either<>(null, requireNonNull(right));
    }

    public boolean isLeft() {
        return left != null;
    }

    public boolean isRight() {
        return right != null;
    }

    public L left() {
        return left;
    }

    public R right() {
        return right;
    }

    // this may get a fold method, but i am not yet sure how to make it really useful

}
