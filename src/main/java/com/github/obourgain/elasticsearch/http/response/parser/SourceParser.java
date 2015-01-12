package com.github.obourgain.elasticsearch.http.response.parser;

import java.io.IOException;
import java.util.Map;
import org.elasticsearch.common.xcontent.XContentParser;

public class SourceParser {

    public static Map<String, Object> source(XContentParser parser) {
        try {
            Map<String, Object> map = parser.map();
            return map;
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse source", e);
        }
    }

}
