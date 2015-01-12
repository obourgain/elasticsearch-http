package com.github.obourgain.elasticsearch.http.response.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Index {

    private String name;
    private Shards shards;

}
