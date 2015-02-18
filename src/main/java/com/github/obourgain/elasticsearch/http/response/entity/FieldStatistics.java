package com.github.obourgain.elasticsearch.http.response.entity;

import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class FieldStatistics {

    private int docCount;
    private long sumDocFreq;
    private long sumTtf;

    public static FieldStatistics fromMap(Map<String, Object> map) {
        int docCount = ((Number) map.get("doc_count")).intValue();
        long sumDocFreq = ((Number) map.get("sum_doc_freq")).longValue();
        long sumTtf = ((Number) map.get("sum_ttf")).longValue();
        return new FieldStatistics(docCount, sumDocFreq, sumTtf);
    }
}
