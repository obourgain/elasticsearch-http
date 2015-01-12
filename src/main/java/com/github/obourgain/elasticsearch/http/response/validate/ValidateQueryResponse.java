package com.github.obourgain.elasticsearch.http.response.validate;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import com.github.obourgain.elasticsearch.http.response.parser.ShardParser;
import com.ning.http.client.Response;
import lombok.Getter;
import lombok.experimental.Builder;

@Builder
@Getter
public class ValidateQueryResponse {

    private Shards shards;
    private boolean valid;

    public static ValidateQueryResponse parse(Response response) {
        try {
            return doParse(response.getResponseBodyAsBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ValidateQueryResponse doParse(byte[] body) throws IOException {
        XContentParser parser = XContentHelper.createParser(body, 0, body.length);

        ValidateQueryResponseBuilder builder = builder();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("valid".equals(currentFieldName)) {
                    builder.valid(parser.booleanValue());
                } else {
                    throw new IllegalStateException("unknown field " + currentFieldName);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("_shards".equals(currentFieldName)) {
                    builder.shards(ShardParser.parseInner(parser));
                }
            }
            // TODO shard failures
        }
        return builder.build();
    }
}
