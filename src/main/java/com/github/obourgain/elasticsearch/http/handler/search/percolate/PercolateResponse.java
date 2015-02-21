package com.github.obourgain.elasticsearch.http.handler.search.percolate;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import com.github.obourgain.elasticsearch.http.response.parser.ShardParser;
import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Getter;
import rx.Observable;

@Builder
@Getter
public class PercolateResponse {

    private Shards shards;
    private long tookInMillis;
    private long total;
    private Matches matches;
    // TODO facet ?
    // TODO aggs ?

    public static Observable<PercolateResponse> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }

    protected static PercolateResponse doParse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            PercolateResponseBuilder builder = builder();
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
