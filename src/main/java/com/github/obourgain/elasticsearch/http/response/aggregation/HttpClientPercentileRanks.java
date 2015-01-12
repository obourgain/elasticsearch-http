package com.github.obourgain.elasticsearch.http.response.aggregation;

import static java.lang.Double.parseDouble;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;

public class HttpClientPercentileRanks extends InternalNumericMetricsAggregation.MultiValue implements PercentileRanks {

    private final Map<String, Object> values;

    public HttpClientPercentileRanks(String name, Map<String, Object> values) {
        super(name);
        this.values = values;
    }

    @Override
    public Type type() {
        return InternalPercentileRanks.TYPE;
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        throw new IllegalStateException("should not be called");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        // TODO keyed
//        if (keyed) {
//            builder.startObject(CommonFields.VALUES);
//            for(int i = 0; i < keys.length; ++i) {
//                String key = String.valueOf(keys[i]);
//                double value = value(keys[i]);
//                builder.field(key, value);
//                if (valueFormatter != null) {
//                    builder.field(key + "_as_string", valueFormatter.format(value));
//                }
//            }
//            builder.endObject();
//        } else {
        builder.startArray(CommonFields.VALUES);
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            builder.startObject();
            builder.field(CommonFields.KEY, entry.getKey());
            builder.field(CommonFields.VALUE, entry.getValue());
            if (valueFormatter != null) {
                builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(toDouble(entry.getValue())));
            }
            builder.endObject();
        }
        builder.endArray();
//        }
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

    @Override
    public Iterator<Percentile> iterator() {
        // note that the functions inverts key and value
        return Iterables.transform(values.entrySet(), new Function<Map.Entry<String, Object>, Percentile>() {
            @Override
            public Percentile apply(final Map.Entry<String, Object> input) {
                return new Percentile() {
                    @Override
                    public double getPercent() {
                        return toDouble(input.getValue());
                    }

                    @Override
                    public double getValue() {
                        return parseDouble(input.getKey());
                    }
                };
            }
        }).iterator();
    }

    @Override
    public double value(String name) {
        return toDouble(values.get(name));
    }

    @Override
    public double percent(double percentile) {
        return value(String.valueOf(percentile));
    }

    /**
     * We may have doubles or Strings in the response map, this methods is an adapter to always have doubles.
     */
    private double toDouble(Object input) {
        if (input instanceof String) {
            return parseDouble((String) input);
        } else if (input instanceof Number) {
            return ((Number) input).doubleValue();
        } else {
            throw new IllegalStateException("expected a String or Number, got " + input.getClass());
        }
    }
}
