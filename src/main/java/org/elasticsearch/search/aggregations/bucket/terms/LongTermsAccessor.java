package org.elasticsearch.search.aggregations.bucket.terms;

import static com.github.obourgain.elasticsearch.http.response.AggregationResultHandler.subAggregations;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.elasticsearch.search.aggregations.InternalAggregations;
import com.github.obourgain.elasticsearch.http.response.AggregationMetaInfos;

public class LongTermsAccessor {

    public static List<InternalTerms.Bucket> createBuckets(AggregationMetaInfos aggregationMetaInfos, List<Map<String, Object>> bucketsAsMaps, boolean showDocumentErrorUpperBound, Number docCountErrorUpperBound) {
        List<InternalTerms.Bucket> buckets = new ArrayList<>();
        for (Map<String, Object> bucketAsMap : bucketsAsMaps) {
            String key = (String) bucketAsMap.get("key_as_string");
            if (key == null) {
                key = String.valueOf(bucketAsMap.get("key"));
            }
            long docCount = ((Number) bucketAsMap.get("doc_count")).longValue();
            InternalAggregations subAggregations = subAggregations(aggregationMetaInfos, bucketAsMap);
            // TODO doc error
            buckets.add(new LongTerms.Bucket(Long.valueOf(key), docCount, subAggregations, showDocumentErrorUpperBound, showDocumentErrorUpperBound ? docCountErrorUpperBound.longValue() : -1));
        }
        return buckets;
    }

}
