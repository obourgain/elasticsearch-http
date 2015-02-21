package com.github.obourgain.elasticsearch.http.response.admin.indices.flush;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import com.github.obourgain.elasticsearch.http.response.parser.ShardParser;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class FlushResponse {

    private Shards shards;
    private final int status;
    private String error;

    public FlushResponse(Shards shards, int status) {
        this.shards = shards;
        this.status = status;
    }

    public FlushResponse(String error, int status) {
        this.status = status;
        this.error = error;
    }

    public static Observable<FlushResponse> parse(ByteBuf content, int status) {
        return Observable.just(doParse(new ByteBufBytesReference(content), status));
    }

    private static FlushResponse doParse(BytesReference bytesReference, int status) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            String error = null;

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("error".equals(currentFieldName)) {
                        error = parser.text();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("_shards".equals(currentFieldName)) {
                        return new FlushResponse(ShardParser.parseInner(parser), status);
                    }
                }
            }
            return new FlushResponse(error, status);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
