package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import org.elasticsearch.common.xcontent.XContentBuilder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public abstract class AbstractBucket {

    protected Aggregations aggregations;

    protected void addSubAgg(String name, XContentBuilder rawAgg) {
        if (aggregations == null) {
            aggregations = new Aggregations();
        }
        aggregations.addRawAgg(name, rawAgg);
    }
}
