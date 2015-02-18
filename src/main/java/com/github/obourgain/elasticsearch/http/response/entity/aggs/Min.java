package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class Min extends AbtractValueAggregation {

    protected Min(String name, double value) {
        super(name, value);
    }

    // TODO value as string ?

    public static Min parse(XContentParser parser, String name) {
        double value = AbtractValueAggregation.parse(parser);
        return new Min(name, value);
    }
}
