package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import org.elasticsearch.common.xcontent.XContentParser;

public class Global extends AbtractSingleBucketAggregation {

    protected Global(String name, long docCount, Aggregations aggregations) {
        super(name, docCount, aggregations);
    }

    public static Global parse(XContentParser parser, String name) {
        ParseResult parseResult = AbtractSingleBucketAggregation.parse(parser);
        return new Global(name, parseResult.getDocCount(), parseResult.getAggregations());
    }
}
