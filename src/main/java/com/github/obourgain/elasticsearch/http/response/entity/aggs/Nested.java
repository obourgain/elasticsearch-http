package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import org.elasticsearch.common.xcontent.XContentParser;

public class Nested extends AbtractSingleBucketAggregation {

    // TODO test is as doc do not show that doc_count is returned whereas bu reading the source, it seems it is

    protected Nested(String name, long docCount, Aggregations aggregations) {
        super(name, docCount, aggregations);
    }

    public static Nested parse(XContentParser parser, String name) {
        ParseResult parseResult = AbtractSingleBucketAggregation.parse(parser);
        return new Nested(name, parseResult.getDocCount(), parseResult.getAggregations());
    }
}
