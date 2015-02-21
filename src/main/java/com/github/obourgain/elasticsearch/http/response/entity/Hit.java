package com.github.obourgain.elasticsearch.http.response.entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class Hit {

    private static final String[] EMPTY = {};
    private String index;
    private String type;
    private String id;
    private float score;
    private long version;
    // TODO keep as byte[] and parse on demand ?
    private byte[] source;
    private List<String> sort = Collections.emptyList();

    public static Hit parseHit(XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();

        XContentParser.Token token;
        String currentFieldName = null;
        HitBuilder builder = builder();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("_id".equals(currentFieldName)) {
                    builder.id(parser.text());
                } else if ("_index".equals(currentFieldName)) {
                    builder.index(parser.text());
                } else if ("_type".equals(currentFieldName)) {
                    builder.type(parser.text());
                } else if ("_version".equals(currentFieldName)) {
                    builder.version(parser.intValue());
                } else if ("_score".equals(currentFieldName)) {
                    builder.score(parser.floatValue());
                }
            } else if ("_source".equals(currentFieldName)) {
                XContentBuilder docBuilder = XContentFactory.contentBuilder(XContentType.JSON);
                docBuilder.copyCurrentStructure(parser);
                builder.source(docBuilder.bytes().array());
            } else if("sort".equals(currentFieldName)) {
                assert parser.currentToken() == XContentParser.Token.START_ARRAY : "expected a START_ARRAY token but was " + parser.currentToken();
                builder.sort(parseSort(parser));
            } else {
                throw new IllegalStateException("unknown field " + currentFieldName);
            }
            // see org.elasticsearch.search.internal.InternalSearchHit
            // TODO fields
            // TODO highlight
            // TODO sort
            // TODO matched_queries
            // TODO _explanation
        }

        return builder.build();
    }

    public static List<Hit> parseHitArray(XContentParser parser) {
        assert parser.currentToken() == XContentParser.Token.START_ARRAY : "expected a START_ARRAY token but was " + parser.currentToken();
        try {
            List<Hit> result = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
                result.add(parseHit(parser));
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> parseSort(XContentParser parser) {
        assert parser.currentToken() == XContentParser.Token.START_ARRAY : "expected a START_ARRAY token but was " + parser.currentToken();
        try {
            List<String> result = new ArrayList<>();
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                result.add(parser.text());
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
