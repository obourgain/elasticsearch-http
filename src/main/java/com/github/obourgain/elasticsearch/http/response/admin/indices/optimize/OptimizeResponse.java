package com.github.obourgain.elasticsearch.http.response.admin.indices.optimize;

import java.io.IOException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import com.github.obourgain.elasticsearch.http.response.parser.ShardParser;
import com.ning.http.client.Response;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class OptimizeResponse {

    private Shards shards;

    public static OptimizeResponse parse(Response response) {
        try {
            return doParse(response.getResponseBodyAsBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static OptimizeResponse doParse(byte[] body) {
        try {
            XContentParser parser = XContentHelper.createParser(body, 0, body.length);

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if(token == XContentParser.Token.START_OBJECT) {
                    if ("_shards".equals(currentFieldName)) {
                        return new OptimizeResponse(ShardParser.parseInner(parser));
                    }
                }
            }
            throw new IllegalStateException("shards not found in response");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
