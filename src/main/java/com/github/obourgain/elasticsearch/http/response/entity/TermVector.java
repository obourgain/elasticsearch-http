package com.github.obourgain.elasticsearch.http.response.entity;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class TermVector {

    private String field;
    private FieldStatistics fieldStatistics;
    private List<Term> terms;

    public static TermVector parse(XContentParser parser) {
        try {
            assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            TermVectorBuilder builder = builder();
            parser.nextToken();
            // the term vector's field
            assert parser.currentToken() == XContentParser.Token.FIELD_NAME : "expected a FIELD_NAME token but was " + parser.currentToken();
            builder.field(parser.text());

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("field_statistics".equals(currentFieldName)) {
                        parser.nextToken();
                        Map<String, Object> map = parser.map();
                        builder.fieldStatistics(FieldStatistics.fromMap(map));
                    } else if ("terms".equals(currentFieldName)) {
                        List<Term> terms = Term.parseTerms(parser);
                        builder.terms(terms);
                    }
                    // throw if unknown ?
                }
            }
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
