package org.elasticsearch.search.aggregations.metrics.sum;

public class InternalSumAccessor {

    public static InternalSum create(String name, double sum) {
        return new InternalSum(name, sum);
    }

}
