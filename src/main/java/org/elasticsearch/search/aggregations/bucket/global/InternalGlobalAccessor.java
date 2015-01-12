package org.elasticsearch.search.aggregations.bucket.global;

import org.elasticsearch.search.aggregations.InternalAggregations;

public class InternalGlobalAccessor {

    public static InternalGlobal create(String name, long docCount, InternalAggregations aggregations) {
        return new InternalGlobal(name, docCount, aggregations);
    }

}
