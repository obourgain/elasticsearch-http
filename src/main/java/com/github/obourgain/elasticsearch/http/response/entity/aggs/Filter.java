package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import org.elasticsearch.common.xcontent.XContentParser;

public class Filter extends AbtractSingleBucketAggregation {

    protected Filter(String name, long docCount, Aggregations aggregations) {
        super(name, docCount, aggregations);
    }

    public static Filter parse(XContentParser parser, String name) {
        ParseResult parseResult = AbtractSingleBucketAggregation.parse(parser);
        return new Filter(name, parseResult.getDocCount(), parseResult.getAggregations());
    }
}
