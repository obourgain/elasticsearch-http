package com.github.obourgain.elasticsearch.http.response.admin.indices.createindex;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.ning.http.client.Response;
import lombok.Getter;
import lombok.experimental.Builder;

@Getter
@Builder
public class CreateIndexResponse {

    private boolean acknowledged;
    private int status;
    private String error;

    public static CreateIndexResponse parse(Response response) {
        try {
            int status = response.getStatusCode();
            return doParse(response.getResponseBodyAsBytes(), status);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static CreateIndexResponse doParse(byte[] body, int status) {
        try {
            XContentParser parser = XContentHelper.createParser(body, 0, body.length);

            CreateIndexResponseBuilder builder = builder();
            builder.status(status);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("acknowledged".equals(currentFieldName)) {
                        builder.acknowledged(parser.booleanValue());
                    } else if ("error".equals(currentFieldName)) {
                        builder.error(parser.text());
                    } else if ("status".equals(currentFieldName)) {
                        // skip, it is set from http status code
                    } else {
                        throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                }
            }
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
