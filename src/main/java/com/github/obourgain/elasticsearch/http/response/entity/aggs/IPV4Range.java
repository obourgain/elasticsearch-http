package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class IPV4Range extends AbtractAggregation {

    private List<Bucket> buckets;

    public IPV4Range(String name) {
        super(name);
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public static IPV4Range parse(XContentParser parser, String name) {
        try {
            IPV4Range range = new IPV4Range(name);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY && "buckets".equals(currentFieldName)) {
                    // not keyed
                    range.buckets = parseBuckets(parser);
                }
            }
            return range;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static List<Bucket> parseBuckets(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<Bucket> result = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                result.add(parseBucket(parser));
            }
        }
        return result;
    }

    protected static Bucket parseBucket(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        Bucket bucket = new Bucket();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("from".equals(currentFieldName)) {
                    bucket.from = parser.doubleValue();
                } else if ("from_as_string".equals(currentFieldName)) {
                    bucket.fromAsString = parser.text();
                } else if ("to".equals(currentFieldName)) {
                    bucket.to = parser.doubleValue();
                } else if ("to_as_string".equals(currentFieldName)) {
                    bucket.toAsString = parser.text();
                } else if ("doc_count".equals(currentFieldName)) {
                    bucket.docCount = parser.longValue();
                } else if ("key".equals(currentFieldName)) {
                    bucket.key = parser.text();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                Pair<String, XContentBuilder> agg = Aggregations.parseInnerAgg(parser, currentFieldName);
                bucket.addSubAgg(agg.getKey(), agg.getValue());
            }
        }
        return bucket;
    }

    @Getter
    @EqualsAndHashCode
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Bucket {
        private String key;
        private Double from;
        private String fromAsString;
        private Double to;
        private String toAsString;
        private long docCount;
        private Aggregations aggregations;

        private void addSubAgg(String name, XContentBuilder rawAgg) {
            if (aggregations == null) {
                aggregations = new Aggregations();
            }
            aggregations.addRawAgg(name, rawAgg);
        }

    }
}
