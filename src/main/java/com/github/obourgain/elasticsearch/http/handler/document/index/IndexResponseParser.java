package com.github.obourgain.elasticsearch.http.handler.document.index;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import io.netty.buffer.ByteBuf;
import rx.Observable;

public class IndexResponseParser {

    public static Observable<IndexResponse> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }

    private static IndexResponse doParse(BytesReference bytesReference) {
        try {
            XContentParser parser = XContentHelper.createParser(bytesReference);

            IndexResponse.IndexResponseBuilder builder = IndexResponse.builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    assert currentFieldName != null;
                    switch (currentFieldName) {
                        case "_index":
                            builder.index(parser.text());
                            break;
                        case "_type":
                            builder.type(parser.text());
                            break;
                        case "_id":
                            builder.id(parser.text());
                            break;
                        case "_version":
                            builder.version(parser.longValue());
                            break;
                        case "created":
                            builder.created(parser.booleanValue());
                            break;
                        default:
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
