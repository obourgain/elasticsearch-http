package com.github.obourgain.elasticsearch.http.response.entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Explanation {
    private float value;
    private String description;
    private List<Explanation> details;

    public static Explanation parseExplanation(XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();

        ExplanationBuilder builder = builder();
        builder.details(Collections.<Explanation>emptyList());
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("description".equals(currentFieldName)) {
                    builder.description(parser.text());
                } else if ("value".equals(currentFieldName)) {
                    builder.value(parser.floatValue());
                } else {
                    throw new IllegalStateException("unknown field " + currentFieldName);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("details".equals(currentFieldName)) {
                    builder.details(parseArray(parser));
                }
            }
        }
        return builder.build();
    }

    protected static List<Explanation> parseArray(XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_ARRAY : "expected a START_ARRAY token but was " + parser.currentToken();

        List<Explanation> result = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            Explanation explanation = parseExplanation(parser);
            result.add(explanation);
        }
        return result;
    }
}
