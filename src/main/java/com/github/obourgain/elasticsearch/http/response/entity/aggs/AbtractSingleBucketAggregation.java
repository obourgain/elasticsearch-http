package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

public abstract class AbtractSingleBucketAggregation extends AbtractAggregation {

    private final long docCount;
    private Aggregations aggregations;

    protected AbtractSingleBucketAggregation(String name, long docCount, Aggregations aggregations) {
        super(name);
        this.docCount = docCount;
        this.aggregations = aggregations;
    }

    public final long getDocCount() {
        return docCount;
    }

    public Aggregations getAggregations() {
        return aggregations;
    }

    protected static ParseResult parse(XContentParser parser) {
        try {
            boolean found = false;
            boolean atSubAggsLevel = false;
            ParseResult parseResult = new ParseResult();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("doc_count".equals(currentFieldName)) {
                        parseResult.docCount = parser.longValue();
                        found = true;
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (atSubAggsLevel) {
                        return innerParse(parser);
                    } else {
                        atSubAggsLevel = true;
                    }
                }
            }
            if (!found) {
                throw new IllegalStateException("value not found in response");
            }
            return parseResult;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static ParseResult innerParse(XContentParser parser) {
        try {
            boolean found = false;
            ParseResult parseResult = new ParseResult();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("doc_count".equals(currentFieldName)) {
                        parseResult.docCount = parser.longValue();
                        found = true;
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    Pair<String, XContentBuilder> agg = Aggregations.parseInnerAgg(parser, currentFieldName);
                    parseResult.aggregations.addRawAgg(agg.getKey(), agg.getValue());
                }
            }
            if (!found) {
                throw new IllegalStateException("value not found in response");
            }
            return parseResult;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Getter
    protected static class ParseResult {
        private long docCount;
        private Aggregations aggregations = new Aggregations();
    }
}
