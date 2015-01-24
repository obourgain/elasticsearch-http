package com.github.obourgain.elasticsearch.http.response.search.count;

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
public class CountResponse {

    private Shards shards;
    private long count;
    private boolean terminatedEarly;

    public static CountResponse parse(Response response) {
        try {
            byte[] body = response.getResponseBodyAsBytes();
            XContentParser parser = XContentHelper.createParser(body, 0, body.length);

            CountResponse.CountResponseBuilder builder = CountResponse.builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("count".equals(currentFieldName)) {
                        builder.count(parser.longValue());
                    } else if ("terminated_early".equals(currentFieldName)) {
                        builder.terminatedEarly(parser.booleanValue());
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
