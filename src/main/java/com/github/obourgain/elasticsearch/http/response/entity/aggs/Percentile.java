package com.github.obourgain.elasticsearch.http.response.entity.aggs;


import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class Percentile {
    protected double key;
    protected double value;
}
