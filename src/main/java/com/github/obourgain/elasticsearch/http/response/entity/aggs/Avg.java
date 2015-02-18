package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class Avg extends AbtractValueAggregation {

    protected Avg(String name, double value) {
        super(name, value);
    }

    // TODO value as string ?

    public static Avg parse(XContentParser parser, String name) {
        double value = AbtractValueAggregation.parse(parser);
        return new Avg(name, value);
    }
}
