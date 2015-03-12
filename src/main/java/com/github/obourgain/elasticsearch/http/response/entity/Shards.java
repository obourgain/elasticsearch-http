package com.github.obourgain.elasticsearch.http.response.entity;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_ARRAY;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class Shards {

    private int total;
    private int successful;
    private int failed;
    private List<ShardSearchFailure> failures = Collections.emptyList();

    public Shards parse(XContentParser parser) {
        // {"total":3,"successful":3,"failed":0}
        try {
            assert parser.currentToken() == START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != END_OBJECT) {
                if (token == FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("total".equals(currentFieldName)) {
                        total = parser.intValue();
                    } else if ("successful".equals(currentFieldName)) {
                        successful = parser.intValue();
                    } else if ("failed".equals(currentFieldName)) {
                        failed = parser.intValue();
                    }
                } else if(token == START_ARRAY) {
                    if ("failures".equals(currentFieldName)) {
                        failures = ShardSearchFailure.parse(parser);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse source", e);
        }
        return this;
    }

    @Override
    public String toString() {
        return "Shards{" +
                "total=" + total +
                ", successful=" + successful +
                ", failed=" + failed +
                '}';
    }
}
