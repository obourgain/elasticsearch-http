package com.github.obourgain.elasticsearch.http.response.parser;

import java.io.IOException;
import java.util.Map;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import com.google.common.base.Preconditions;

public class ShardParser {

    public static Shards parse(XContentParser parser) {
        // {"_shards":{"total":3,"successful":3,"failed":0}}
        try {
            // maybe caller should place the parser at the correct position ?
            assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            parser.nextToken();
            assert "_shards".equals(parser.currentName()) : "current token name is " + parser.currentName();
            parser.nextToken();
            assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            return Shards.fromMap(parser.map());
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse source", e);
        }
    }

    public static Shards parseInner(XContentParser parser) {
        // {"total":3,"successful":3,"failed":0}
        try {
            assert "_shards".equals(parser.currentName()) : "current token name is " + parser.currentName();
            assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            return Shards.fromMap(parser.map());
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse source", e);
        }
    }

}
