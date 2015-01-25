package com.github.obourgain.elasticsearch.http.response.admin.indices.optimize;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import com.github.obourgain.elasticsearch.http.response.parser.ShardParser;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;
import rx.Observable;

@Getter
@AllArgsConstructor
public class OptimizeResponse {

    private Shards shards;
    private int status;
    private String error;

    public OptimizeResponse(Shards shards, int status) {
        this.shards = shards;
        this.status = status;
    }

    public OptimizeResponse(String error, int status) {
        this.status = status;
        this.error = error;
    }

    public static Observable<OptimizeResponse> parse(ByteBuf content, int status) {
        return Observable.just(doParse(new ByteBufBytesReference(content), status));
    }

    private static OptimizeResponse doParse(BytesReference bytesReference, int status) {
        try {
            XContentParser parser = XContentHelper.createParser(bytesReference);

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
                        return new OptimizeResponse(ShardParser.parseInner(parser), status);
                    }
                }
            }
            return new OptimizeResponse(error, status);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
