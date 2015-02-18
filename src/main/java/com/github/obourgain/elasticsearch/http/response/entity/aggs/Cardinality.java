package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;

public class Cardinality extends AbtractAggregation {

    private final long value;

    protected Cardinality(String name, long value) {
        super(name);
        this.value = value;
    }

    public final double getValue() {
        return value;
    }

    protected static Cardinality parse(XContentParser parser, String name) {
        try {
            boolean found = false;
            long value = 0;
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("value".equals(currentFieldName)) {
                        value = parser.longValue();
                        found = true;
                    }
                }
            }
            if (!found) {
                throw new IllegalStateException("value not found in response");
            }
            return new Cardinality(name, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
