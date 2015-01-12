package org.elasticsearch.search.aggregations.bucket.histogram;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import com.github.obourgain.elasticsearch.http.response.AggregationMetaInfos;
import com.github.obourgain.elasticsearch.http.response.AggregationResultHandler;

public class InternalDateHistogramAccessor {

    public static InternalDateHistogram create(String name, List<InternalDateHistogram.Bucket> buckets, InternalOrder order, long minDocCount,
                                               InternalHistogram.EmptyBucketInfo emptyBucketInfo, @Nullable ValueFormatter formatter, boolean keyed) {
        return new InternalDateHistogram(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed);
    }

    // InternalDateHistogram is not really practical due to non public classes
    public static List<DateHistogram.Bucket> createBuckets(AggregationMetaInfos aggregationMetaInfos, List<Map<String, Object>> buckets) {
        List<DateHistogram.Bucket> result = new ArrayList<>();
        // TODO this won't handle keyed agg
        for (Map<String, Object> bucketAsMap : buckets) {
            // TODO pre offset
            // TODO this should be able to handle different date formats
            String keyAsString = (String) bucketAsMap.get("key_as_string");
            long key = ((Number) bucketAsMap.get("key")).longValue();
            long docCount = ((Number)bucketAsMap.get("doc_count")).longValue();
            List<InternalAggregation> subAggregationsAsList = new ArrayList<>();
            for (Map.Entry<String, AggregationMetaInfos> child : aggregationMetaInfos.getChildren().entrySet()) {
                Map<String, Object> childAsMap = (Map<String, Object>) bucketAsMap.get(child.getValue().getName());
                InternalAggregation subAggregation = AggregationResultHandler.buildAggregation(child.getValue(), childAsMap);
                subAggregationsAsList.add(subAggregation);
            }
            InternalAggregations subAggregations = new InternalAggregations(subAggregationsAsList);
            InternalDateHistogram.Bucket bucket = InternalDateHistogram.FACTORY.createBucket(key, docCount, subAggregations, null);
//            DateHistogram.Bucket bucket = new HttpClientDateHistogramBucket(key, docCount, subAggregations);
            result.add(bucket);
        }
        return result;
    }

    // must cast because the consumer will require an InternalOrder but declared type is Histogram.Order
    public static InternalOrder COUNT_ASC() {
        return (InternalOrder) InternalOrder.COUNT_ASC;
    }

    public static InternalOrder COUNT_DESC() {
        return (InternalOrder) InternalOrder.COUNT_DESC;
    }

    public static InternalOrder KEY_ASC() {
        return (InternalOrder) InternalOrder.KEY_ASC;
    }

    public static InternalOrder KEY_DESC() {
        return (InternalOrder) InternalOrder.KEY_DESC;
    }

}
