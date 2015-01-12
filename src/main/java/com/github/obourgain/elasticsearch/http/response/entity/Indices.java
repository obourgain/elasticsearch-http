package com.github.obourgain.elasticsearch.http.response.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.Getter;

@Getter
public class Indices implements Iterable<Index> {

    private List<Index> indices = Collections.emptyList();

    @Override
    public Iterator<Index> iterator() {
        return indices.iterator();
    }

    private Indices(List<Index> indices) {
        this.indices = indices;
    }

    public static Indices fromMap(Map<String, Shards> map) {
        List<Index> indices= new ArrayList<>();
        for (Map.Entry<String, Shards> entry : map.entrySet()) {
            indices.add(new Index(entry.getKey(), entry.getValue()));
        }
        return new Indices(indices);
    }
}
