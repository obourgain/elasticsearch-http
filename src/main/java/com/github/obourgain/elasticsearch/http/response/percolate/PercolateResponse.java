package com.github.obourgain.elasticsearch.http.response.percolate;

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
public class PercolateResponse {

    private Shards shards;
    private long tookInMillis;
    private long total;
    private Matches matches;
    // TODO facet ?
    // TODO aggs ?

    public static PercolateResponse parse(Response response) {
        try {
            byte[] body = response.getResponseBodyAsBytes();
            return doParse(body);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static PercolateResponse doParse(byte[] body) throws IOException {
        XContentParser parser = XContentHelper.createParser(body, 0, body.length);

        PercolateResponseBuilder builder = PercolateResponse.builder();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("took".equals(currentFieldName)) {
                    builder.tookInMillis(parser.longValue());
                } else if ("total".equals(currentFieldName)) {
                    builder.total(parser.longValue());
                } else {
                    throw new IllegalStateException("unknown field " + currentFieldName);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("_shards".equals(currentFieldName)) {
                    builder.shards(ShardParser.parseInner(parser));
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("matches".equals(currentFieldName)) {
                    builder.matches(Matches.parse(parser));
                }
            }
        }
        return builder.build();

    }
}
