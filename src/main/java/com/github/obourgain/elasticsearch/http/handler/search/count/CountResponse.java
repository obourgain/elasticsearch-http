package com.github.obourgain.elasticsearch.http.handler.search.count;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.ShardFailure;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class CountResponse {

    private Shards shards;
    private long count;
    private boolean terminatedEarly;
    private List<ShardFailure> shardFailures = Collections.emptyList();

    public static Observable<CountResponse> parse(ByteBuf content) {
        return Observable.just(new CountResponse().parse(new ByteBufBytesReference(content)));
    }

    private CountResponse parse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("count".equals(currentFieldName)) {
                        count = parser.longValue();
                    } else if ("terminated_early".equals(currentFieldName)) {
                        terminatedEarly = parser.booleanValue();
                    } else {
                        throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("failures".equals(currentFieldName)) {
                        shardFailures = ShardFailure.parse(parser);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("_shards".equals(currentFieldName)) {
                        shards = new Shards().parse(parser);
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
