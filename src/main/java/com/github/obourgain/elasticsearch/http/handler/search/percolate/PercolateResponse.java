package com.github.obourgain.elasticsearch.http.handler.search.percolate;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class PercolateResponse {

    private Shards shards;
    private long tookInMillis;
    private long total;
    private Matches matches;
    // TODO facet ?
    // TODO aggs ?

    public static Observable<PercolateResponse> parse(ByteBuf content) {
        return Observable.just(new PercolateResponse().parse(new ByteBufBytesReference(content)));
    }

    protected PercolateResponse parse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("took".equals(currentFieldName)) {
                        tookInMillis = parser.longValue();
                    } else if ("total".equals(currentFieldName)) {
                        total = parser.longValue();
                    } else {
                        throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("_shards".equals(currentFieldName)) {
                        shards = new Shards().parse(parser);
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("matches".equals(currentFieldName)) {
                        matches = Matches.parse(parser);
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
