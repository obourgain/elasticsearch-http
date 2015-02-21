package com.github.obourgain.elasticsearch.http.response.entity.aggs;

public abstract class AbstractAggregation implements Aggregation {

    protected String name;

    public AbstractAggregation() {
    }

    public AbstractAggregation(String name) {
        this.name = name;
    }

    public final String getName() {
        return name;
    }
}
