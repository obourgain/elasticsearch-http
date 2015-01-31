package com.github.obourgain.elasticsearch.http.handler.search.exists;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.experimental.Builder;
import rx.Observable;

@Builder
@Getter
public class ExistsResponse {

    private boolean exists;

    public static Observable<ExistsResponse> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }

    private static ExistsResponse doParse(BytesReference bytesReference) {
        try {
            XContentParser parser = XContentHelper.createParser(bytesReference);

            ExistsResponseBuilder builder = ExistsResponse.builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("exists".equals(currentFieldName)) {
                        builder.exists(parser.booleanValue());
                    } else {
                        throw new IllegalStateException("unknown field " + currentFieldName);
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
