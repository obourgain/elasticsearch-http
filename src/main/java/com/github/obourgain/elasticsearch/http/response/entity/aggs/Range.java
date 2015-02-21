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

public class Range extends AbtractAggregation {

    private List<Bucket> buckets;

    public Range(String name) {
        super(name);
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public static Range parse(XContentParser parser, String name) {
        try {
            Range range = new Range(name);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT && "buckets".equals(currentFieldName)) {
                    // keyed
                    range.buckets = parseKeyedBuckets(parser);
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

    protected static List<Bucket> parseKeyedBuckets(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<Bucket> result = new ArrayList<>();
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.text();
            } else if (token == XContentParser.Token.START_OBJECT) {
                Bucket bucket = parseBucket(parser);
                bucket.key = currentFieldName;
                result.add(bucket);
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
                } else if ("to".equals(currentFieldName)) {
                    bucket.to = parser.doubleValue();
                } else if ("doc_count".equals(currentFieldName)) {
                    bucket.docCount = parser.longValue();
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
        private Double to;
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
