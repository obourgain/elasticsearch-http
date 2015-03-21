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

@Getter
public class Terms extends AbstractAggregation {

    private long sumOtherDocCount = -1;
    private long docCountErrorUpperBound = -1;
    private List<Bucket> buckets;

    public Terms(String name) {
        super(name);
    }

    public static Terms parse(XContentParser parser, String name) {
        try {
            Terms terms = new Terms(name);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("doc_count_error_upper_bound".equals(currentFieldName)) {
                        terms.docCountErrorUpperBound = parser.longValue();
                    } else if ("sum_other_doc_count".equals(currentFieldName)) {
                        terms.sumOtherDocCount = parser.longValue();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("buckets".equals(currentFieldName)) {
                        terms.buckets = parseBuckets(parser);
                    }
                }
            }
            return terms;
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
                if ("doc_count_error_upper_bound".equals(currentFieldName)) {
                    bucket.docCountErrorUpperBound = parser.longValue();
                } else if ("key".equals(currentFieldName)) {
                    bucket.key = parser.text();
                } else if ("key_as_string".equals(currentFieldName)) {
                    // ignore for now
                } else if ("doc_count".equals(currentFieldName)) {
                    bucket.docCount = parser.longValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT && currentFieldName != null) { // parse as sub agg only if not at first level
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
        private long docCountErrorUpperBound;
        private String key;
        //        private String keyAsString;
        private long docCount;
    }
}
