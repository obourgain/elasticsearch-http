package com.github.obourgain.elasticsearch.http.response.entity.suggest;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_ARRAY;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import lombok.Getter;

@Getter
public class Completion implements Suggestion {

    private final String name;

    private String text;
    private int offset;
    private int length;
    private List<Option> options = Collections.emptyList();

    public Completion(String name) {
        this.name = name;
    }

    @Getter
    public static class Option {
        private String text;
        private float score;
        private String payload; // may be an arbitrary JSON object, let user take care of it, maybe add accessors like asMap() or store it as byte[] and add asString()
    }

    public static Completion parse(XContentParser parser, String name) {
        try {
            Completion completion = new Completion(name);
            Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != END_OBJECT) {
                if (token == FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if("text".equals(currentFieldName)) {
                        completion.text = parser.text();
                    } else if("offset".equals(currentFieldName)) {
                        completion.offset = parser.intValue();
                    } else if("length".equals(currentFieldName)) {
                        completion.length = parser.intValue();
                    }
                } else if (token == START_ARRAY && "options".equals(currentFieldName)) {
                    completion.options = parseOptions(parser);
                }
            }
            return completion;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    protected static List<Option> parseOptions(XContentParser parser) throws IOException {
        Token token;
        List<Option> result = new ArrayList<>();
        while ((token = parser.nextToken()) != Token.END_ARRAY) {
            if (token == Token.START_OBJECT) {
                result.add(parseOption(parser));
            }
        }
        return result;
    }

    protected static Option parseOption(XContentParser parser) throws IOException {
        Token token;
        String currentFieldName = null;
        Option option = new Option();
        while ((token = parser.nextToken()) != Token.END_OBJECT) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("text".equals(currentFieldName)) {
                    option.text = parser.text();
                } else if ("score".equals(currentFieldName)) {
                    option.score = parser.floatValue();
                }
            } else if (token == Token.START_OBJECT && "payload".equals(currentFieldName)) {
                try (XContentBuilder docBuilder = XContentFactory.contentBuilder(XContentType.JSON)) {
                    docBuilder.copyCurrentStructure(parser);
                    option.payload = docBuilder.bytes().toUtf8();
                }
            }
        }
        return option;
    }
}
