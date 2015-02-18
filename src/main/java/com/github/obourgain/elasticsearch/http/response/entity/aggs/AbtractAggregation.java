package com.github.obourgain.elasticsearch.http.response.entity.aggs;

public abstract class AbtractAggregation implements Aggregation {

    private final String name;

    public AbtractAggregation(String name) {
        this.name = name;
    }

    public final String getName() {
        return name;
    }
}
