package com.github.obourgain.elasticsearch.http.response.entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class Token {

    private String payload;
    private Integer position;
    private Integer startOffset;
    private Integer endOffset;

    public Token parse(XContentParser parser) {
        try {
            assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("end_offset".equals(currentFieldName)) {
                        endOffset = parser.intValue();
                    } else if ("payload".equals(currentFieldName)) {
                        payload = parser.text();
                    } else if ("position".equals(currentFieldName)) {
                        position = parser.intValue();
                    } else if ("start_offset".equals(currentFieldName)) {
                        startOffset = parser.intValue();
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Token> parseList(XContentParser parser) {
        try {
            assert parser.currentToken() == XContentParser.Token.START_ARRAY : "expected a START_ARRAY token but was " + parser.currentToken();
            List<Token> tokens = new ArrayList<>();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
                Token parsed = new Token().parse(parser);
                tokens.add(parsed);
            }
            return tokens;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
