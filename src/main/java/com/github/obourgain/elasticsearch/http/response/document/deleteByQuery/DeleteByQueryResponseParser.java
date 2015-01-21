package com.github.obourgain.elasticsearch.http.response.document.deleteByQuery;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.response.entity.Indices;
import com.github.obourgain.elasticsearch.http.response.parser.IndicesParser;
import com.ning.http.client.Response;

public class DeleteByQueryResponseParser {

    public static DeleteByQueryResponse parse(Response response) {
        try {
            return doParse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static DeleteByQueryResponse doParse(Response response) throws IOException {
        byte[] body = response.getResponseBodyAsBytes();
        XContentParser parser = XContentHelper.createParser(body, 0, body.length);

        // TODO parse failures

        DeleteByQueryResponse.DeleteByQueryResponseBuilder builder = DeleteByQueryResponse.builder();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if(token == XContentParser.Token.START_OBJECT) {
                if("_indices".equals(currentFieldName)) {
                    Indices indices = IndicesParser.parse(parser);
                    builder.indices(indices);
                }
            }
        }
        return builder.build();
    }

}
