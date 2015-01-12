package org.elasticsearch.search.aggregations.bucket.geogrid;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import com.github.obourgain.elasticsearch.http.response.AggregationMetaInfos;
import com.github.obourgain.elasticsearch.http.response.AggregationResultHandler;

public class InternalGeoHashGridAccessor {

    public static InternalGeoHashGrid create(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        // TODO not keyed
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) aggregationAsMap.get("buckets");
        List<InternalGeoHashGrid.Bucket> result = new ArrayList<>();
        for (Map<String, Object> bucket : buckets) {
            String key = (String) bucket.get("key");
            long docCount = ((Number) bucket.get("doc_count")).longValue();

            // TODO factorize this chunk
            List<InternalAggregation> subAggregationsAsList = new ArrayList<>();
            for (Map.Entry<String, AggregationMetaInfos> child : aggregationMetaInfos.getChildren().entrySet()) {
                Map<String, Object> childAsMap = (Map<String, Object>) bucket.get(child.getValue().getName());
                InternalAggregation subAggregation = AggregationResultHandler.buildAggregation(child.getValue(), childAsMap);
                subAggregationsAsList.add(subAggregation);
            }
            InternalAggregations subAggregations = new InternalAggregations(subAggregationsAsList);

            GeoPoint geoCodeAsGeoPoint = GeoHashUtils.decode(key);
            long geoCodeAsLong = GeoHashUtils.encodeAsLong(geoCodeAsGeoPoint.getLat(), geoCodeAsGeoPoint.getLon(), key.length());
            // TODO formatter
            result.add(new InternalGeoHashGrid.Bucket(geoCodeAsLong, docCount, subAggregations));
        }
        // TODO required size
        return new InternalGeoHashGrid(aggregationMetaInfos.getName(), 0, result);
    }

}
