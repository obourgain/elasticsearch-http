package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import org.elasticsearch.common.xcontent.XContentParser;

public class Missing extends AbtractSingleBucketAggregation {

    protected Missing(String name, long docCount, Aggregations aggregations) {
        super(name, docCount, aggregations);
    }

    public static Missing parse(XContentParser parser, String name) {
        ParseResult parseResult = AbtractSingleBucketAggregation.parse(parser);
        return new Missing(name, parseResult.getDocCount(), parseResult.getAggregations());
    }
}
