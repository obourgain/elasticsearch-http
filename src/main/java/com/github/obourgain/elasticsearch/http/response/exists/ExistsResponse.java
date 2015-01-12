package com.github.obourgain.elasticsearch.http.response.exists;

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
public class ExistsResponse {

    private boolean exists;

    public static ExistsResponse parse(Response response) {
        try {
            byte[] body = response.getResponseBodyAsBytes();
            XContentParser parser = XContentHelper.createParser(body, 0, body.length);

            ExistsResponseBuilder builder = ExistsResponse.builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("exists".equals(currentFieldName)) {
                        builder.exists(parser.booleanValue());
                    } else {
                        throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                }
                // TODO shard failures
            }
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
