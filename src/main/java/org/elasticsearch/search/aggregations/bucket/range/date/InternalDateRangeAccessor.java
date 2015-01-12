package org.elasticsearch.search.aggregations.bucket.range.date;

import static com.github.obourgain.elasticsearch.http.response.AggregationResultHandler.getAsNumber;
import static com.github.obourgain.elasticsearch.http.response.AggregationResultHandler.subAggregations;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.elasticsearch.search.aggregations.InternalAggregations;
import com.github.obourgain.elasticsearch.http.response.AggregationMetaInfos;

public class InternalDateRangeAccessor {

    public static InternalDateRange create(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Object bucketsAsObject = aggregationAsMap.get("buckets");
        List<InternalDateRange.Bucket> ranges = new ArrayList<>();
        boolean keyed;
        // TODO factorize with IPv4range & date range ?
        if (bucketsAsObject instanceof Map) { // keyed
            keyed = true;
            Map<String, Map<String, Object>> buckets = (Map<String, Map<String, Object>>) bucketsAsObject;
            for (Map.Entry<String, Map<String, Object>> entry : buckets.entrySet()) {
                String key = entry.getKey();
                Map<String, Object> bucket = entry.getValue();
                Number from = getAsNumber(bucket, "from");
                Number to = getAsNumber(bucket, "to");
                long docCount = getAsNumber(bucket, "doc_count").longValue();

                double resultFrom = from != null ? from.doubleValue() : Double.NEGATIVE_INFINITY;
                double resultTo = to != null ? to.doubleValue() : Double.POSITIVE_INFINITY;
                InternalAggregations subAggregations = subAggregations(aggregationMetaInfos, bucket);
                // TODO value formatter
                ranges.add(new InternalDateRange.Bucket(key, resultFrom, resultTo, docCount, subAggregations, null));
            }
        } else if (bucketsAsObject instanceof List) { // not keyed
            keyed = false;
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) bucketsAsObject;
            for (Map<String, Object> bucket : buckets) {
                String key = (String) bucket.get("key");
                Number from = getAsNumber(bucket, "from");
                Number to = getAsNumber(bucket, "to");
                long docCount = getAsNumber(bucket, "doc_count").longValue();

                double resultFrom = from != null ? from.doubleValue() : Double.NEGATIVE_INFINITY;
                double resultTo = to != null ? to.doubleValue() : Double.POSITIVE_INFINITY;
                InternalAggregations subAggregations = subAggregations(aggregationMetaInfos, bucket);
                // TODO value formatter
                ranges.add(new InternalDateRange.Bucket(key, resultFrom, resultTo, docCount, subAggregations, null));
            }
        } else {
            throw new IllegalStateException("buckets of type " + bucketsAsObject.getClass() + " are not supported for ipv4range, please report this bug");
        }
        return new InternalDateRange(aggregationMetaInfos.getName(), ranges, null, keyed);
    }

}
