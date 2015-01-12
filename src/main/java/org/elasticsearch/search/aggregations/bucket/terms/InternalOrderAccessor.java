package org.elasticsearch.search.aggregations.bucket.terms;

public class InternalOrderAccessor {

    public static InternalOrder COUNT_ASC() {
        return InternalOrder.COUNT_ASC;
    }

    public static InternalOrder COUNT_DESC() {
        return InternalOrder.COUNT_DESC;
    }

    public static InternalOrder TERM_ASC() {
        return InternalOrder.TERM_ASC;
    }

    public static InternalOrder TERM_DESC() {
        return InternalOrder.TERM_DESC;
    }

}
