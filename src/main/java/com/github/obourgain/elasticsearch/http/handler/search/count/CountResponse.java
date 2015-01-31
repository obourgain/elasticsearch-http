package com.github.obourgain.elasticsearch.http.handler.search.count;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import com.github.obourgain.elasticsearch.http.response.parser.ShardParser;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.experimental.Builder;
import rx.Observable;

@Builder
@Getter
public class CountResponse {

    private Shards shards;
    private long count;
    private boolean terminatedEarly;

    public static Observable<CountResponse> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }

    private static CountResponse doParse(BytesReference bytesReference) {
        try {
            XContentParser parser = XContentHelper.createParser(bytesReference);

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
