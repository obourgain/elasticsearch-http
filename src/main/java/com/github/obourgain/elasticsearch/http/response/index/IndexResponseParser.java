package com.github.obourgain.elasticsearch.http.response.index;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.ning.http.client.Response;

public class IndexResponseParser {

    public static IndexResponse parse(Response response) {
        try {
            return doParse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static IndexResponse doParse(Response response) throws IOException {
        byte[] body = response.getResponseBodyAsBytes();
        XContentParser parser = XContentHelper.createParser(body, 0, body.length);

        IndexResponse.IndexResponseBuilder builder = IndexResponse.builder();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case "_index":
                        builder.index(parser.text());
                        break;
                    case "_type":
                        builder.type(parser.text());
                        break;
                    case "_id":
                        builder.id(parser.text());
                        break;
                    case "_version":
                        builder.version(parser.longValue());
                        break;
                    case "created":
                        builder.created(parser.booleanValue());
                        break;
                    default:throw new IllegalStateException("unknown field " + currentFieldName);
                }
            }
        }
        return builder.build();
    }

}
