package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.search.aggregations.InternalAggregations;

public class InternalFilterAccessor {

    public static InternalFilter create(String name, long docCount, InternalAggregations aggregations) {
        return new InternalFilter(name, docCount, aggregations);
    }

}
