package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

public abstract class AbstractSingleBucketAggregation<T extends AbstractSingleBucketAggregation<?>> extends AbstractAggregation {

    private long docCount;
    private Aggregations aggregations;

    protected AbstractSingleBucketAggregation() {

    }

    public final long getDocCount() {
        return docCount;
    }

    public Aggregations getAggregations() {
        return aggregations;
    }

    protected T parse(XContentParser parser) {
        return parse(parser, this.name);
    }

    protected T parse(XContentParser parser, String name) {
        try {
            this.name = name;
            boolean atSubAggsLevel = false;
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("doc_count".equals(currentFieldName)) {
                        this.docCount = parser.longValue();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (atSubAggsLevel) {
                        return innerParse(parser);
                    } else {
                        atSubAggsLevel = true;
                    }
                }
            }
            throw new IllegalStateException("value not found in response");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected T innerParse(XContentParser parser) {
        try {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("doc_count".equals(currentFieldName)) {
                        this.docCount = parser.longValue();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    Pair<String, XContentBuilder> agg = Aggregations.parseInnerAgg(parser, currentFieldName);
                    if(this.aggregations == null) {
                        this.aggregations = new Aggregations();
                    }
                    this.aggregations.addRawAgg(agg.getKey(), agg.getValue());
                }
            }
            return (T) this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
