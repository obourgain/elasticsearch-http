package com.github.obourgain.elasticsearch.http.response.parser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.response.entity.Indices;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;

public class IndicesParser {

    public static Indices parse(XContentParser parser) {
        Map<String, Shards> result = new HashMap<>();
        try {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if(token == XContentParser.Token.START_OBJECT) {
                    Shards shards = ShardParser.parse(parser);
                    result.put(currentFieldName, shards);
                }
            }
            return Indices.fromMap(result);
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse source", e);
        }
    }

}
