package com.github.obourgain.elasticsearch.http.response.search.explain;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.response.entity.Explanation;
import com.ning.http.client.Response;
import lombok.Getter;
import lombok.experimental.Builder;

@Builder
@Getter
public class ExplainResponse {

    private String index;
    private String type;
    private String id;
    private boolean matched;
    private Explanation explanation;

    public static ExplainResponse parse(Response response) {
        try {
            byte[] body = response.getResponseBodyAsBytes();
            return doParse(body);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static ExplainResponse doParse(byte[] body) throws IOException {
        XContentParser parser = XContentHelper.createParser(body, 0, body.length);

        ExplainResponseBuilder builder = builder();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("_index".equals(currentFieldName)) {
                    builder.index(parser.text());
                } else if ("_type".equals(currentFieldName)) {
                    builder.type(parser.text());
                } else if ("_id".equals(currentFieldName)) {
                    builder.id(parser.text());
                } else if ("matched".equals(currentFieldName)) {
                    builder.matched(parser.booleanValue());
                } else {
                    throw new IllegalStateException("unknown field " + currentFieldName);
                }
            } else if(token == XContentParser.Token.START_OBJECT) {
                if ("explanation".equals(currentFieldName)) {
                    builder.explanation(Explanation.parseExplanation(parser));
                }
            }
        }
        return builder.build();
    }
}
