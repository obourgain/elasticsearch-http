package com.github.obourgain.elasticsearch.http.response.admin.indices.mapping.put;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Getter;
import rx.Observable;

@Getter
@Builder
public class PutMappingResponse {

    private boolean acknowledged;
    private int status;
    private String error;

    public static Observable<PutMappingResponse> parse(ByteBuf content, int status) {
        return Observable.just(doParse(new ByteBufBytesReference(content), status));
    }

    private static PutMappingResponse doParse(BytesReference bytesReference, int status) {
        try {
            XContentParser parser = XContentHelper.createParser(bytesReference);

            PutMappingResponseBuilder builder = builder();
            builder.status(status);
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("acknowledged".equals(currentFieldName)) {
                        builder.acknowledged(parser.booleanValue());
                    } else if ("error".equals(currentFieldName)) {
                        builder.error(parser.text());
                    } else if ("status".equals(currentFieldName)) {
                        // skip, it is set from http status code
                    } else {
                        throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                }
            }
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
