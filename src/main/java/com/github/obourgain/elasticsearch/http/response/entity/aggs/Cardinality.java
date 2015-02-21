package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;

public class Cardinality extends AbstractAggregation {

    private long value;

    public final double getValue() {
        return value;
    }

    protected Cardinality parse(XContentParser parser, String name) {
        try {
            this.name = name;
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("value".equals(currentFieldName)) {
                        value = parser.longValue();
                        return this;
                    }
                }
            }
            throw new IllegalStateException("value not found in response");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
