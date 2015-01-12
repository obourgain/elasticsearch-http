package org.elasticsearch.search.aggregations.bucket.histogram;

import java.util.List;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

public class InternalHistogramAccessor {

    public static <B extends InternalHistogram.Bucket> InternalHistogram<B> create(String name, List<B> buckets, InternalOrder order, long minDocCount,
                                               InternalHistogram.EmptyBucketInfo emptyBucketInfo, @Nullable ValueFormatter formatter, boolean keyed) {
        return new InternalHistogram<>(name, buckets, order, minDocCount, emptyBucketInfo, formatter, keyed);
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
