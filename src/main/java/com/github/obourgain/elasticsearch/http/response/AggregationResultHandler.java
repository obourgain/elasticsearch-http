package com.github.obourgain.elasticsearch.http.response;

import static java.lang.Double.NaN;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilterAccessor;
import org.elasticsearch.search.aggregations.bucket.filters.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGridAccessor;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobalAccessor;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogramAccessor;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogramAccessor;
import org.elasticsearch.search.aggregations.bucket.missing.InternalMissingAccessor;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.nested.InternalReverseNested;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.range.date.InternalDateRangeAccessor;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.InternalGeoDistanceAccessor;
import org.elasticsearch.search.aggregations.bucket.range.ipv4.InternalIPv4Range;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTermsAccessor;
import org.elasticsearch.search.aggregations.bucket.terms.InternalOrderAccessor;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTermsAccessor;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.InternalExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSumAccessor;
import org.elasticsearch.search.aggregations.metrics.tophits.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import com.github.obourgain.elasticsearch.http.response.aggregation.HttpClientCardinality;
import com.github.obourgain.elasticsearch.http.response.aggregation.HttpClientGeoBounds;
import com.github.obourgain.elasticsearch.http.response.aggregation.HttpClientPercentileRanks;
import com.github.obourgain.elasticsearch.http.response.aggregation.HttpClientPercentiles;

public class AggregationResultHandler {

    public static InternalAggregation buildAggregation(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        String aggType = aggregationMetaInfos.getType();

        switch (aggType) {
            ////////// these are "bucket" aggs //////////
            // we are lucky, the user provided infos about the data type
            case "terms":
            case "sterms":
                return stringTerms(aggregationMetaInfos, aggregationAsMap);
            case "lterms":
                return longTerms(aggregationMetaInfos, aggregationAsMap);
            case "dterms":
                return doubleTerms(aggregationMetaInfos, aggregationAsMap);
            case "umterms":
                return unmappedTerms(aggregationMetaInfos, aggregationAsMap);
            case "histo":
            case "histogram":
                return histogram(aggregationMetaInfos, aggregationAsMap);
            case "date_histogram":
            case "dhisto":
                return dateHistogram(aggregationMetaInfos, aggregationAsMap);
            case "filter":
                return filter(aggregationMetaInfos, aggregationAsMap);
            case "filters":
                return filters(aggregationMetaInfos, aggregationAsMap);
            case "geo_distance":
            case "gdist":
                return geoDistance(aggregationMetaInfos, aggregationAsMap);
            case "geohash_grid":
            case "ghcells":
                return geoHashGrid(aggregationMetaInfos, aggregationAsMap);
            case "global":
                return global(aggregationMetaInfos, aggregationAsMap);
            case "ip_range":
            case "iprange":
                return ipRange(aggregationMetaInfos, aggregationAsMap);
            case "missing":
                return missing(aggregationMetaInfos, aggregationAsMap);
            case "nested":
                return nested(aggregationMetaInfos, aggregationAsMap);
            case "range":
                return range(aggregationMetaInfos, aggregationAsMap);
            case "date_range":
            case "drange":
                return dateRange(aggregationMetaInfos, aggregationAsMap);
            case "reverse_nested":
                return reverseNested(aggregationMetaInfos, aggregationAsMap);
//            case "significant_terms":
//            case "sigsterms":
//            case "siglterms":
//            case "umsigterms":
            // TODO
            case "top_hits":
                return topHits(aggregationMetaInfos, aggregationAsMap);


            ////////// below are "metrics" aggs //////////
            case "value_count":
            case "vcount":
                return count(aggregationMetaInfos, aggregationAsMap);
            case "avg":
                return avg(aggregationMetaInfos, aggregationAsMap);
            case "cardinality":
                return cardinality(aggregationMetaInfos, aggregationAsMap);
            case "extended_stats":
                return extendedStats(aggregationMetaInfos, aggregationAsMap);
            case "geo_bounds":
                return geoBounds(aggregationMetaInfos, aggregationAsMap);
            case "max":
                return max(aggregationMetaInfos, aggregationAsMap);
            case "min":
                return min(aggregationMetaInfos, aggregationAsMap);
            case "percentiles":
                return percentiles(aggregationMetaInfos, aggregationAsMap);
            case "percentile_ranks":
                return percentileRanks(aggregationMetaInfos, aggregationAsMap);
            case "stats":
                return stats(aggregationMetaInfos, aggregationAsMap);
            case "sum":
                return sum(aggregationMetaInfos, aggregationAsMap);
            default:
                throw new IllegalStateException("aggregation with type " + aggType + " is not supported");
        }
    }

    private static InternalAggregation topHits(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Map<String, Object> hitsAsMap = (Map<String, Object>) aggregationAsMap.get("hits");

        Number total = getAsNumber(hitsAsMap, "total");
        Number maxScore = getAsNumber(hitsAsMap, "max_score");
        List<Map<String, Object>> hits = (List<Map<String, Object>>) hitsAsMap.get("hits");
        InternalSearchHit[] internalSearchHits = ResponseWrapper.processInternalSearchHits(hits);
        float actualMaxScore = maxScore != null ? maxScore.floatValue() : Float.NaN;
        InternalSearchHits searchHits = new InternalSearchHits(internalSearchHits, total.longValue(), actualMaxScore);

        return new InternalTopHits(aggregationMetaInfos.getName(), searchHits);
    }

    private static InternalAggregation reverseNested(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        long docCount = getAsNumber(aggregationAsMap, "doc_count").longValue();
        return new InternalReverseNested(aggregationMetaInfos.getName(), docCount, subAggregations(aggregationMetaInfos, aggregationAsMap));
    }

    private static InternalAggregation dateRange(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        return InternalDateRangeAccessor.create(aggregationMetaInfos, aggregationAsMap);
    }

    private static InternalAggregation range(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Object bucketsAsObject = aggregationAsMap.get("buckets");
        List<InternalRange.Bucket> ranges = new ArrayList<>();
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
                ranges.add(new InternalRange.Bucket(key, resultFrom, resultTo, docCount, subAggregations, ValueFormatter.RAW));
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
                ranges.add(new InternalRange.Bucket(key, resultFrom, resultTo, docCount, subAggregations, ValueFormatter.RAW));
            }
        } else {
            throw new IllegalStateException("buckets of type " + bucketsAsObject.getClass() + " are not supported for ipv4range, please report this bug");
        }
        // TODO this value formatter is ok ?
        return new InternalRange<>(aggregationMetaInfos.getName(), ranges, ValueFormatter.RAW, keyed);
    }

    private static InternalAggregation nested(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        long docCount = getAsNumber(aggregationAsMap, "doc_count").longValue();
        return new InternalNested(aggregationMetaInfos.getName(), docCount, subAggregations(aggregationMetaInfos, aggregationAsMap));
    }

    private static InternalAggregation missing(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        long docCount = getAsNumber(aggregationAsMap, "doc_count").longValue();
        return InternalMissingAccessor.create(aggregationMetaInfos.getName(), docCount, subAggregations(aggregationMetaInfos, aggregationAsMap));
    }

    private static InternalAggregation ipRange(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Object bucketsAsObject = aggregationAsMap.get("buckets");
        List<InternalIPv4Range.Bucket> ranges = new ArrayList<>();
        boolean keyed;
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
                ranges.add(new InternalIPv4Range.Bucket(key, resultFrom, resultTo, docCount, subAggregations));
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
                ranges.add(new InternalIPv4Range.Bucket(key, resultFrom, resultTo, docCount, subAggregations));
            }
        } else {
            throw new IllegalStateException("buckets of type " + bucketsAsObject.getClass() + " are not supported for ipv4range, please report this bug");
        }
        return new InternalIPv4Range(aggregationMetaInfos.getName(), ranges, keyed);
    }

    private static InternalAggregation global(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        long docCount = getAsNumber(aggregationAsMap, "doc_count").longValue();
        return InternalGlobalAccessor.create(aggregationMetaInfos.getName(), docCount, subAggregations(aggregationMetaInfos, aggregationAsMap));
    }

    private static InternalAggregation geoHashGrid(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        return InternalGeoHashGridAccessor.create(aggregationMetaInfos, aggregationAsMap);
    }

    private static InternalAggregation geoDistance(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        return InternalGeoDistanceAccessor.create(aggregationMetaInfos, aggregationAsMap);
    }

    private static InternalAggregation filter(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        long docCount = getAsNumber(aggregationAsMap, "doc_count").longValue();
        InternalAggregations subAggregations = subAggregations(aggregationMetaInfos, aggregationAsMap);
        return InternalFilterAccessor.create(aggregationMetaInfos.getName(), docCount, subAggregations);
    }

    private static InternalAggregation filters(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Object bucketsAsObject = aggregationAsMap.get("buckets");
        List<InternalFilters.Bucket> resultBucketList = new ArrayList<>();
        if (bucketsAsObject instanceof Map) { // keyed
            for (Map.Entry<String, Map<String, Object>> bucketEntry : ((Map<String, Map<String, Object>>) bucketsAsObject).entrySet()) {
                Map<String, Object> bucketAsMap = bucketEntry.getValue();
                long docCount = getAsNumber(bucketAsMap, "doc_count").longValue();
                InternalAggregations subAggregations = subAggregations(aggregationMetaInfos, bucketAsMap);
                resultBucketList.add(new InternalFilters.Bucket(bucketEntry.getKey(), docCount, subAggregations));
            }
        } else if (bucketsAsObject instanceof List) { // not keyed
            for (Map<String, Object> bucketAsMap : (List<Map<String, Object>>) bucketsAsObject) {
                long docCount = getAsNumber(bucketAsMap, "doc_count").longValue();
                InternalAggregations subAggregations = subAggregations(aggregationMetaInfos, bucketAsMap);
                // TODO does transport client return a generated name ?
                resultBucketList.add(new InternalFilters.Bucket(null, docCount, subAggregations));
            }
        } else {
            throw new IllegalStateException("buckets of type " + bucketsAsObject.getClass() + " are not supported for filter, please report this bug");
        }

        return new InternalFilters(aggregationMetaInfos.getName(), resultBucketList, false);
    }

    private static InternalAggregation histogram(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        List<Map<String, Object>> buckets = getBuckets(aggregationAsMap);
        List<InternalHistogram.Bucket> result = new ArrayList<>();
        // TODO this won't handle keyed agg
        for (Map<String, Object> bucketAsMap : buckets) {
            long key = getAsNumber(bucketAsMap, "key").longValue();
            long docCount = getAsNumber(bucketAsMap, "doc_count").longValue();
            InternalAggregations subAggregations = subAggregations(aggregationMetaInfos, bucketAsMap);
            InternalHistogram.Bucket bucket = new InternalHistogram.Bucket(key, docCount, null, subAggregations);
            result.add(bucket);
        }
        // TODO order
        // TODO empty bucket info
        // TODO keyed
        return InternalHistogramAccessor.create(aggregationMetaInfos.getName(), result, InternalHistogramAccessor.COUNT_ASC(), 1, null, null, false);
    }

    private static InternalAggregation dateHistogram(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        List<Map<String, Object>> buckets = getBuckets(aggregationAsMap);
        List result = InternalDateHistogramAccessor.createBuckets(aggregationMetaInfos, buckets);
        // TODO order
        // TODO empty bucket info
        // TODO keyed
        return InternalDateHistogramAccessor.create(aggregationMetaInfos.getName(), result, InternalDateHistogramAccessor.COUNT_ASC(), 1, null, null, false);
    }

    private static InternalAggregation percentileRanks(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Map<String, Object> values = getAs(aggregationAsMap, "values", Map.class);
        return new HttpClientPercentileRanks(aggregationMetaInfos.getName(), values);
    }

    private static InternalAggregation percentiles(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Map<String, Object> values = getAs(aggregationAsMap, "values", Map.class);
        return new HttpClientPercentiles(aggregationMetaInfos.getName(), values);
    }

    private static InternalAggregation sum(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Number value = getAsNumber(aggregationAsMap, "value");
        return InternalSumAccessor.create(aggregationMetaInfos.getName(), value != null ? value.doubleValue() : Double.NEGATIVE_INFINITY);
    }

    private static InternalAggregation max(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Number value = getAsNumber(aggregationAsMap, "value");
        return new InternalMax(aggregationMetaInfos.getName(), value != null ? value.doubleValue() : Double.NEGATIVE_INFINITY);
    }

    private static InternalAggregation min(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Number value = getAsNumber(aggregationAsMap, "value");
        return new InternalMin(aggregationMetaInfos.getName(), value != null ? value.doubleValue() : Double.POSITIVE_INFINITY);
    }

    private static InternalAggregation geoBounds(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        // this fails on backward compatibility assertions because it is my own custom class
        Map bounds = getAs(aggregationAsMap, "bounds", Map.class);
        GeoPoint topLeftPoint = null;
        GeoPoint bottomRightPoint = null;
        if (bounds != null) {
            Map topLeft = getAs(bounds, "top_left", Map.class);
            Number topLeftLat = getAsNumber(topLeft, "lat");
            Number topLeftLon = getAsNumber(topLeft, "lon");

            Map bottomRight = getAs(bounds, "bottom_right", Map.class);
            Number bottomRightLat = getAsNumber(bottomRight, "lat");
            Number bottomRightLon = getAsNumber(bottomRight, "lon");

            topLeftPoint = new GeoPoint(topLeftLat.doubleValue(), topLeftLon.doubleValue());
            bottomRightPoint = new GeoPoint(bottomRightLat.doubleValue(), bottomRightLon.doubleValue());
        }
        return new HttpClientGeoBounds(aggregationMetaInfos.getName(), topLeftPoint, bottomRightPoint);
    }

    private static InternalAggregation stats(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Number count = getAsNumber(aggregationAsMap, "count");
        Number sum = getAsNumber(aggregationAsMap, "sum");
        Number min = getAsNumber(aggregationAsMap, "min");
        Number max = getAsNumber(aggregationAsMap, "max");
        return new InternalStats(aggregationMetaInfos.getName(), count != null ? count.longValue() : 0,
                sum != null ? sum.doubleValue() : 0,
                min != null ? min.doubleValue() : Double.POSITIVE_INFINITY,
                max != null ? max.doubleValue() : Double.NEGATIVE_INFINITY);
    }

    private static InternalAggregation extendedStats(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Number count = getAsNumber(aggregationAsMap, "count");
        Number sum = getAsNumber(aggregationAsMap, "sum");
        Number min = getAsNumber(aggregationAsMap, "min");
        Number max = getAsNumber(aggregationAsMap, "max");
        Number sumOfSqrs = getAsNumber(aggregationAsMap, "sum_of_squares");
        return new InternalExtendedStats(aggregationMetaInfos.getName(),
                count != null ? count.longValue() : 0,
                sum != null ? sum.doubleValue() : 0,
                min != null ? min.doubleValue() : Double.POSITIVE_INFINITY,
                max != null ? max.doubleValue() : Double.NEGATIVE_INFINITY,
                sumOfSqrs != null ? sumOfSqrs.doubleValue() : 0);
    }

    private static InternalAggregation cardinality(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Number value = getAsNumber(aggregationAsMap, "value");
        return new HttpClientCardinality(aggregationMetaInfos.getName(), value);
    }

    private static InternalAggregation avg(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        Number value = getAsNumber(aggregationAsMap, "value");
        // InternalAvg uses sum / count to compute the avg but i only have the avg, let's hack something
        // sum & count are not exposed, so I am lucky
        double valueAsDouble = value != null ? value.doubleValue() : NaN;
        return new InternalAvg(aggregationMetaInfos.getName(), valueAsDouble, 1);
    }

    private static InternalValueCount count(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> bucketAsMap) {
        Number count = getAsNumber(bucketAsMap, "value");
        return new InternalValueCount(aggregationMetaInfos.getName(), count.longValue());
    }

    private static InternalAggregation unmappedTerms(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        return null;
    }

    private static InternalAggregation doubleTerms(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        List<Map<String, Object>> bucketsAsMaps = getBuckets(aggregationAsMap);

        Number docCountErrorUpperBound = getAs(aggregationAsMap, "doc_count_error_upper_bound", Number.class);
        boolean showDocumentErrorUpperBound = docCountErrorUpperBound != null && docCountErrorUpperBound.longValue() != 0;

        List<InternalTerms.Bucket> buckets = DoubleTermsAccessor.createBuckets(aggregationMetaInfos, bucketsAsMaps, showDocumentErrorUpperBound, docCountErrorUpperBound);
        // TODO showTermDocCountError & docCountError
//        return new DoubleTerms(aggregationMetaInfos.getName(), InternalOrderAccessor.COUNT_ASC(), null, -1, -1, 0, buckets, false, 0);
        return null;
    }

    private static InternalAggregation longTerms(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        List<Map<String, Object>> bucketsAsMaps = getBuckets(aggregationAsMap);

        Number docCountErrorUpperBound = getAs(aggregationAsMap, "doc_count_error_upper_bound", Number.class);
        boolean showDocumentErrorUpperBound = docCountErrorUpperBound != null && docCountErrorUpperBound.longValue() != 0;

        List<InternalTerms.Bucket> buckets = LongTermsAccessor.createBuckets(aggregationMetaInfos, bucketsAsMaps, showDocumentErrorUpperBound, docCountErrorUpperBound);
        // TODO showTermDocCountError & docCountError
//        return new LongTerms(aggregationMetaInfos.getName(), InternalOrderAccessor.COUNT_ASC(), null, -1, -1, 0, buckets, false, 0);
        return null;
    }

    private static InternalAggregation stringTerms(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> aggregationAsMap) {
        List<Map<String, Object>> bucketsAsMaps = getBuckets(aggregationAsMap);

        Number docCountErrorUpperBound = getAs(aggregationAsMap, "doc_count_error_upper_bound", Number.class);
        boolean showDocumentErrorUpperBound = docCountErrorUpperBound != null && docCountErrorUpperBound.longValue() != 0;

        List<InternalTerms.Bucket> buckets = new ArrayList<>();
        for (Map<String, Object> bucketAsMap : bucketsAsMaps) {
            String key = getAs(bucketAsMap, "key_as_string", String.class);
            if (key == null) {
                // for string agg, we have only the key field, for term aggs on number we have both key and key_as_string
                key = String.valueOf(bucketAsMap.get("key"));
            }
            long docCount = getAs(bucketAsMap, "doc_count", Number.class).longValue();
            InternalAggregations subAggregations = subAggregations(aggregationMetaInfos, bucketAsMap);
            buckets.add(new StringTerms.Bucket(new BytesRef(key), docCount, subAggregations, showDocumentErrorUpperBound, 0));
        }
        // TODO some values are hardcoded
//        return new StringTerms(aggregationMetaInfos.getName(), InternalOrderAccessor.COUNT_ASC(), -1, -1, 0, buckets, showDocumentErrorUpperBound, showDocumentErrorUpperBound ? docCountErrorUpperBound.longValue() : 0);
        return null;
    }

    public static InternalAggregations subAggregations(AggregationMetaInfos aggregationMetaInfos, Map<String, Object> bucketAsMap) {
        List<InternalAggregation> subAggregationsAsList = new ArrayList<>();
        for (Map.Entry<String, AggregationMetaInfos> child : aggregationMetaInfos.getChildren().entrySet()) {
            InternalAggregation subAggregation = buildAggregation(child.getValue(), getAsStringObjectMap(bucketAsMap, child.getValue().getName()));
            subAggregationsAsList.add(subAggregation);
        }
        return new InternalAggregations(subAggregationsAsList);
    }

    private static List<Map<String, Object>> getBuckets(Map<String, Object> aggregationAsMap) {
        return (List<Map<String, Object>>) aggregationAsMap.get("buckets");
    }

    @Nullable
    private static Map<String, Map<String, Object>> getAsNestedStringToMapMap(Map map, String key) {
        return (Map<String, Map<String, Object>>) getAs(map, key, Map.class);
    }

    @Nullable
    private static Map<String, Object> getAsStringObjectMap(Map map, String key) {
        return (Map<String, Object>) getAs(map, key, Map.class);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private static <T> T getAs(Map<String, Object> map, String key, Class<T> type) {
        return (T) map.get(key);
    }

    @Nullable
    public static Number getAsNumber(Map<String, Object> map, String key) {
        return getAs(map, key, Number.class);
    }

    @Nullable
    private static String getAsString(Map<String, Object> map, String key) {
        return getAs(map, key, String.class);
    }

}
