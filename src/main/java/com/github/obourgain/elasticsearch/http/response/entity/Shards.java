package com.github.obourgain.elasticsearch.http.response.entity;

import java.util.Map;
import lombok.Getter;

@Getter
public class Shards {

    private final int total;
    private final int successful;
    private final int failed;

    public Shards(int total, int successful, int failed) {
        this.total = total;
        this.successful = successful;
        this.failed = failed;
    }

    public static Shards fromMap(Map<String, Object> map) {
        int total = (int) map.get("total");
        int successful = (int) map.get("successful");
        int failed = (int) map.get("failed");
        return new Shards(total, successful, failed);
    }

}
