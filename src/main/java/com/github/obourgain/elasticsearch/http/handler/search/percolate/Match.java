package com.github.obourgain.elasticsearch.http.handler.search.percolate;

import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.response.entity.Highlight;
import lombok.Getter;

@Getter
public class Match {

    private String index;
    private String id;
    private Float score;
    private Map<String, Highlight> highlights;

    public Match parse(XContentParser parser) {
        try {
            assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("_index".equals(currentFieldName)) {
                        index = parser.text();
                    } else if ("_id".equals(currentFieldName)) {
                        id=parser.text();
                    } else if ("_score".equals(currentFieldName)) {
                        score=parser.floatValue();
                    }
                } else if (token == START_OBJECT && "highlight".equals(currentFieldName)) {
                    highlights = Highlight.parseHighlights(parser);
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
