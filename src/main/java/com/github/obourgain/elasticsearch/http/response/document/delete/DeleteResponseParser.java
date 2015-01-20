package com.github.obourgain.elasticsearch.http.response.document.delete;

import static org.slf4j.LoggerFactory.getLogger;
import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.slf4j.Logger;
import com.ning.http.client.Response;

public class DeleteResponseParser {

    private static final Logger logger = getLogger(DeleteResponseParser.class);

    public static DeleteResponse parse(Response response) {
        try {
            return doParse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static DeleteResponse doParse(Response response) throws IOException {
        byte[] body = response.getResponseBodyAsBytes();
        if (logger.isTraceEnabled()) {
            logger.trace("Parsing {}", new String(body, 0, body.length));
        }
        XContentParser parser = XContentHelper.createParser(body, 0, body.length);

        DeleteResponse.DeleteResponseBuilder builder = DeleteResponse.builder();
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
                    case "found":
                        builder.found(parser.booleanValue());
                        break;
                    default:
                        throw new IllegalStateException("unknown field " + currentFieldName);
                }
            }
        }
        return builder.build();
    }

}
