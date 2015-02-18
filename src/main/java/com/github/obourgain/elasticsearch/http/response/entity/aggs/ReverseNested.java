package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import org.elasticsearch.common.xcontent.XContentParser;

public class ReverseNested extends AbtractSingleBucketAggregation {

    // TODO test is as doc do not show that doc_count is returned whereas bu reading the source, it seems it is

    protected ReverseNested(String name, long docCount, Aggregations aggregations) {
        super(name, docCount, aggregations);
    }

    public static ReverseNested parse(XContentParser parser, String name) {
        ParseResult parseResult = AbtractSingleBucketAggregation.parse(parser);
        return new ReverseNested(name, parseResult.getDocCount(), parseResult.getAggregations());
    }
}
