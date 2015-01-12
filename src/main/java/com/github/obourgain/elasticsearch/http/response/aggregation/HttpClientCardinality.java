package com.github.obourgain.elasticsearch.http.response.aggregation;

import java.io.IOException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;

/**
 * We can not rebuild the InternalCardinality as it requires to rebuild the HyperLogLogPlusPlus
 * After test methods that check backward compatible serialization will not work with this class
 */
public class HttpClientCardinality extends InternalNumericMetricsAggregation.SingleValue implements Cardinality {

    public final static Type TYPE = new Type("cardinality");

    private final Number value;

    public HttpClientCardinality(String name, Number value) {
        super(name);
        this.value = value;
    }

    @Override
    public long getValue() {
        return value.longValue();
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        throw new IllegalStateException("should not be called");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final long cardinality = getValue();
        builder.field(CommonFields.VALUE, cardinality);
        if (valueFormatter != null) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(cardinality));
        }
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new IllegalStateException("should not be called");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new IllegalStateException("should not be called");
    }
}
