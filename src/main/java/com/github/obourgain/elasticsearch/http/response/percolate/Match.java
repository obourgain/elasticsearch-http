package com.github.obourgain.elasticsearch.http.response.percolate;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;
import lombok.experimental.Builder;

@Builder
@Getter
public class Match {

    private String index;
    private String id;
    private Float score;

    public static Match parse(XContentParser parser) {
        try {
            assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            MatchBuilder builder = builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("_index".equals(currentFieldName)) {
                        builder.index(parser.text());
                    } else if ("_id".equals(currentFieldName)) {
                        builder.id(parser.text());
                    } else if ("_score".equals(currentFieldName)) {
                        builder.score(parser.floatValue());
                    }
                }
            }
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
