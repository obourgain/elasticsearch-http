package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class Max extends AbtractValueAggregation {

    protected Max(String name, double value) {
        super(name, value);
    }

    // TODO value as string ?

    public static Max parse(XContentParser parser, String name) {
        double value = AbtractValueAggregation.parse(parser);
        return new Max(name, value);
    }
}
