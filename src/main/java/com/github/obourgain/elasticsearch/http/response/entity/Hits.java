package com.github.obourgain.elasticsearch.http.response.entity;

import java.io.IOException;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;
import lombok.experimental.Builder;

@Getter
@Builder
public class Hits {

    private long total;
    private List<Hit> hits;
    private Float maxScore; // may be null if score is NaN

    public static Hits parse(XContentParser parser) {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();

        try {
            XContentParser.Token token;
            String currentFieldName = null;
            HitsBuilder builder = builder();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("total".equals(currentFieldName)) {
                        builder.total(parser.longValue());
                    } else if ("max_score".equals(currentFieldName)) {
                        builder.maxScore(parser.floatValue());
                    }
                } else if ("hits".equals(currentFieldName)) {
                    List<Hit> hitList = Hit.parseHitArray(parser);
                    builder.hits(hitList);
                }
            }
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
