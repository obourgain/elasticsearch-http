package org.elasticsearch.search.aggregations.bucket.range.geodistance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import com.github.obourgain.elasticsearch.http.response.AggregationMetaInfos;
import com.github.obourgain.elasticsearch.http.response.AggregationResultHandler;

public class InternalGeoDistanceAccessor {

    public static InternalGeoDistance create(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        // TODO not keyed
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) aggregationAsMap.get("buckets");
        List<InternalGeoDistance.Bucket> result = new ArrayList<>();
        for (Map<String, Object> bucket : buckets) {
            String key = (String) bucket.get("key");
            Number to = (Number) bucket.get("to");
            Number from = (Number) bucket.get("from");
            long docCount = ((Number) bucket.get("doc_count")).longValue();
            String unit = (String) bucket.get("unit");

            double resultTo = to != null ? to.doubleValue() : Double.POSITIVE_INFINITY;
            double resultFrom = from != null ? from.doubleValue() : 0;

            // TODO factorize this chunk
            List<InternalAggregation> subAggregationsAsList = new ArrayList<>();
            for (Map.Entry<String, AggregationMetaInfos> child : aggregationMetaInfos.getChildren().entrySet()) {
                Map<String, Object> childAsMap = (Map<String, Object>) bucket.get(child.getValue().getName());
                InternalAggregation subAggregation = AggregationResultHandler.buildAggregation(child.getValue(), childAsMap);
                subAggregationsAsList.add(subAggregation);
            }
            InternalAggregations subAggregations = new InternalAggregations(subAggregationsAsList);

            // TODO formatter
            result.add(new InternalGeoDistance.Bucket(key, resultFrom, resultTo, docCount, subAggregations, null));
        }
        return new InternalGeoDistance(aggregationMetaInfos.getName(), result, null, false);
    }

}
