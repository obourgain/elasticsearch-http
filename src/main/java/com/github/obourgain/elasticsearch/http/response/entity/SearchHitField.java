package com.github.obourgain.elasticsearch.http.response.entity;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_ARRAY;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class SearchHitField {

    private String name;
    private List<String> values = new ArrayList<>();
    ;

    public Object getValue() {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.get(0);
    }

    public SearchHitField parse(XContentParser parser) throws IOException {
        assert parser.currentToken() == FIELD_NAME : "expected a FIELD_NAME token but was " + parser.currentToken();

        name = parser.text();

        XContentParser.Token token;
        while ((token = parser.nextToken()) != END_ARRAY) {
            if (token.isValue()) {
                values.add(parser.text());
            }
        }
        return this;
    }
}
