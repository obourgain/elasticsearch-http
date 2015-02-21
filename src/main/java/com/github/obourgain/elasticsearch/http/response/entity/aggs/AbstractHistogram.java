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

public abstract class AbstractHistogram<T extends AbstractHistogram<?>> extends AbstractAggregation {

    private List<Bucket> buckets;

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public T parse(XContentParser parser, String name) {
        try {
            this.name = name;
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT && "buckets".equals(currentFieldName)) {
                    // keyed
                    buckets = parseKeyedBuckets(parser);
                } else if (token == XContentParser.Token.START_ARRAY && "buckets".equals(currentFieldName)) {
                    // not keyed
                    buckets = parseBuckets(parser);
                }
            }
            return (T) this;
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
                bucket.keyAsString = currentFieldName;
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
                if ("key".equals(currentFieldName)) {
                    bucket.key = parser.longValue();
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
    @EqualsAndHashCode(callSuper = true)
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Bucket extends AbstractBucket {
        private String keyAsString;
        private long key;
        private long docCount;
    }
}
