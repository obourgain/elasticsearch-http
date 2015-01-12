package org.elasticsearch.search.aggregations.bucket.missing;

import org.elasticsearch.search.aggregations.InternalAggregations;

public class InternalMissingAccessor {

    public static InternalMissing create(String name, long docCount, InternalAggregations aggregations) {
        return new InternalMissing(name, docCount, aggregations);
    }

}
