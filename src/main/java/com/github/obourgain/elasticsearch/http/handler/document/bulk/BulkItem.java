package com.github.obourgain.elasticsearch.http.handler.document.bulk;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;

@Getter
public class BulkItem {

    private String opType;
    private String index;
    private String type;
    private String id;
    private Long version; // may be null

    private boolean failed;
    private int status;
    private String error;

    private Boolean found;

    public BulkItem parse(XContentParser parser) {
        try {
            assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();
            parser.nextToken();
            // the op type's field
            assert parser.currentToken() == XContentParser.Token.FIELD_NAME : "expected a FIELD_NAME token but was " + parser.currentToken();
            opType=parser.text();

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("_index".equals(currentFieldName)) {
                        index=parser.text();
                    } else if ("_type".equals(currentFieldName)) {
                        type=parser.text();
                    } else if ("_id".equals(currentFieldName)) {
                        id=parser.text();
                    } else if ("_version".equals(currentFieldName)) {
                        version=parser.longValue();
                    } else if ("status".equals(currentFieldName)) {
                        status=parser.intValue();
                    } else if ("error".equals(currentFieldName)) {
                        error=parser.text();
                        failed=true;
                    } else if ("found".equals(currentFieldName)) {
                        found=parser.booleanValue();
                    }
                }
            }
            // advance to consume the second END_OBJECT
            assert parser.currentToken() == XContentParser.Token.END_OBJECT : "expected a END_OBJECT token but was " + parser.currentToken();
            parser.nextToken();
            assert parser.currentToken() == XContentParser.Token.END_OBJECT : "expected a END_OBJECT token but was " + parser.currentToken();
            parser.nextToken();
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
