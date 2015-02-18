package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;

public abstract class AbtractValueAggregation extends AbtractAggregation {

    private final double value;

    protected AbtractValueAggregation(String name, double value) {
        super(name);
        this.value = value;
    }

    public final double getValue() {
        return value;
    }

    protected static double parse(XContentParser parser) {
        try {
            boolean found = false;
            double value = 0;
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("value".equals(currentFieldName)) {
                        value = parser.doubleValue();
                        found = true;
                    }
                }
            }
            if (!found) {
                throw new IllegalStateException("value not found in response");
            }
            return value;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
