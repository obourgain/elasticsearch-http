package com.github.obourgain.elasticsearch.http.response.aggregation;

import java.io.IOException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.geobounds.InternalGeoBounds;

public class HttpClientGeoBounds extends InternalMetricsAggregation implements GeoBounds {

    private final GeoPoint topLeft;
    private final GeoPoint bottomRight;

    public HttpClientGeoBounds(String name, GeoPoint topLeft, GeoPoint bottomRight) {
        super(name);
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
    }

    @Override
    public GeoPoint topLeft() {
        return topLeft;
    }

    @Override
    public GeoPoint bottomRight() {
        return bottomRight;
    }

    @Override
    public Type type() {
        return InternalGeoBounds.TYPE;
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        throw new IllegalStateException("should not be called");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        GeoPoint topLeft = topLeft();
        GeoPoint bottomRight = bottomRight();
        if (topLeft != null) {
            builder.startObject("bounds");
            builder.startObject("top_left");
            builder.field("lat", topLeft.lat());
            builder.field("lon", topLeft.lon());
            builder.endObject();
            builder.startObject("bottom_right");
            builder.field("lat", bottomRight.lat());
            builder.field("lon", bottomRight.lon());
            builder.endObject();
            builder.endObject();
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
