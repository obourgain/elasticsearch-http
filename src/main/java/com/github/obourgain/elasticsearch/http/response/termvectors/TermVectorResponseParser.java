package com.github.obourgain.elasticsearch.http.response.termvectors;

import java.io.IOException;
import org.assertj.core.util.VisibleForTesting;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.response.entity.TermVector;
import com.ning.http.client.Response;

public class TermVectorResponseParser {

    public static TermVectorResponse parse(Response response) {
        try {
            byte[] body = response.getResponseBodyAsBytes();
            return doParse(body);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    protected static TermVectorResponse doParse(byte[] body) throws IOException {
        System.out.println(new String(body));
        XContentParser parser = XContentHelper.createParser(body, 0, body.length);

        TermVectorResponse.TermVectorResponseBuilder builder = TermVectorResponse.builder();
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
                    case "found":
                        builder.found(parser.booleanValue());
                        break;
                    default:
                        throw new IllegalStateException("unknown field " + currentFieldName);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("term_vectors".equals(currentFieldName)) {
                    TermVector termVector = TermVector.parse(parser);
                    builder.termVector(termVector);
                }
            }
        }
        return builder.build();
    }

}
