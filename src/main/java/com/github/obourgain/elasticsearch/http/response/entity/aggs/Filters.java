package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class Filters extends AbstractAggregation {

    private List<Bucket> buckets = new ArrayList<>();

    public Filters(String name) {
        super(name);
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public static Filters parse(XContentParser parser, String name) {
        try {
            Filters filters = new Filters(name);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    // named filters
                    if ("buckets".equals(currentFieldName)) {
                        filters.buckets = parseBuckets(parser);
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    // anonymous filter
                    if ("buckets".equals(currentFieldName)) {
                        filters.buckets = parseAnonymousBuckets(parser);
                    }
                }
            }
            return filters;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static List<Bucket> parseBuckets(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        List<Bucket> result = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }else if (token == XContentParser.Token.START_OBJECT) {
                Bucket bucket = parseBucket(parser);
                bucket.key = currentFieldName;
                result.add(bucket);
            }
        }
        return result;
    }

    protected static List<Bucket> parseAnonymousBuckets(XContentParser parser) throws IOException {
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
                if ("key".equals(currentFieldName)) {
                    bucket.key = parser.text();
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
        @Nullable
        private String key;
        //        private String keyAsString;
        private long docCount;
    }
}
