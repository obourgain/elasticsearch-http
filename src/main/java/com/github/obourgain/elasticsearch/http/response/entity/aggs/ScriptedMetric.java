package com.github.obourgain.elasticsearch.http.response.entity.aggs;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class ScriptedMetric extends AbstractAggregation {

    private final String value;

    protected ScriptedMetric(String name, String value) {
        super(name);
        this.value = value;
    }

    public static ScriptedMetric parse(XContentParser parser, String name) {
        try {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("value".equals(currentFieldName)) {
                        String value = parser.text();
                        return new ScriptedMetric(name, value);
                    }
                }
            }
            throw new IllegalStateException("value not found in response");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
