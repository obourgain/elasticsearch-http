package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class Sum extends AbtractValueAggregation {

    protected Sum(String name, double value) {
        super(name, value);
    }

    // TODO value as string ?

    public static Sum parse(XContentParser parser, String name) {
        double value = AbtractValueAggregation.parse(parser);
        return new Sum(name, value);
    }
}
