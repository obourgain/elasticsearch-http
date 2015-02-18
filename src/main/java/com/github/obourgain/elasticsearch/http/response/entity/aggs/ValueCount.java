package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class ValueCount extends AbtractValueAggregation {

    protected ValueCount(String name, double value) {
        super(name, value);
    }

    // TODO value as string ?

    public static ValueCount parse(XContentParser parser, String name) {
        double value = AbtractValueAggregation.parse(parser);
        return new ValueCount(name, value);
    }
}
