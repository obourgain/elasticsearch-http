package com.github.obourgain.elasticsearch.http.handler.search.percolate;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.handler.document.multiget.Either;
import com.github.obourgain.elasticsearch.http.handler.search.multipercolate.MultiPercolateResponseError;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import com.github.obourgain.elasticsearch.http.response.entity.aggs.Aggregations;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;
import rx.Observable;

@Getter
@AllArgsConstructor
public class PercolateResponse {

    private Shards shards;
    private long tookInMillis;
    private long total;
    private Matches matches;
    private Aggregations aggregations;

    public static Observable<PercolateResponse> parse(ByteBuf content) {
        return Observable.just(parse(new ByteBufBytesReference(content)));
    }

    protected static PercolateResponse parse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            // converting the result of a percolate request, so it is safe to get the right
            return doParse(parser).right();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Either<MultiPercolateResponseError, PercolateResponse> doParse(XContentParser parser) throws IOException {
        Shards shards = null;
        long tookInMillis = 0;
        long total = 0;
        Matches matches = null;
        Aggregations aggregations = null;
        String error = null;

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
                } else if ("error".equals(currentFieldName)) {
                    error = parser.text();
                } else {
                    throw new IllegalStateException("unknown field " + currentFieldName);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("_shards".equals(currentFieldName)) {
                    shards = new Shards().parse(parser);
                } else if ("aggregations".equals(currentFieldName)) {
                    aggregations = Aggregations.parse(parser);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("matches".equals(currentFieldName)) {
                    matches = new Matches().parse(parser);
                }
            }
        }
        if(error == null) {
            return Either.right(new PercolateResponse(shards, tookInMillis, total, matches, aggregations));
        } else {
            return Either.left(new MultiPercolateResponseError(error));
        }
    }
}
