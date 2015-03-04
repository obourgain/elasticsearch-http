package com.github.obourgain.elasticsearch.http.response.entity;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class Hits implements Iterable<Hit> {

    private long total;
    private List<Hit> hits;
    private Float maxScore; // may be null if score is NaN

    public Hits parse(XContentParser parser) {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();

        try {
            XContentParser.Token token;
            String currentFieldName = null;
            hits = Collections.emptyList();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("total".equals(currentFieldName)) {
                        total =parser.longValue();
                    } else if ("max_score".equals(currentFieldName)) {
                        maxScore=parser.floatValue();
                    }
                } else if ("hits".equals(currentFieldName)) {
                    hits= Hit.parseHitArray(parser);
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Hit> iterator() {
        return hits.iterator();
    }

    public Hit getAt(int i) {
        return hits.get(i);
    }
}
