package com.github.obourgain.elasticsearch.http.response.entity;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_ARRAY;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class Highlight {

    private String name;
    private List<String> values = new ArrayList<>();

    public String getValue() {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.get(0);
    }

    public Highlight parse(XContentParser parser) throws IOException {
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

    public static Map<String, Highlight> parseHighlights(XContentParser parser) {
        try {
            assert parser.currentToken() == START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            assert parser.currentName().equals("highlight") : "expected a current name to be 'highlight' but was " + parser.currentName();
            Map<String, Highlight> result = new HashMap<>();
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                Highlight highlight = new Highlight().parse(parser);
                result.put(highlight.getName(), highlight);
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "Highlight{" +
                "name='" + name + '\'' +
                ", values=" + values +
                '}';
    }
}
