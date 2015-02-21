package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;

public abstract class AbstractValueAggregation<T extends AbstractValueAggregation<?>> extends AbstractAggregation {

    private double value;

    public final double getValue() {
        return value;
    }

    protected T parse(XContentParser parser, String name) {
        try {
            this.name = name;
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("value".equals(currentFieldName)) {
                        this.value = parser.doubleValue();
                        return (T) this;
                    }
                }
            }
            throw new IllegalStateException("value not found in response");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
