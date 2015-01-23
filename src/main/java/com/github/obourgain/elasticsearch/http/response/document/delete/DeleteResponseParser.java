package com.github.obourgain.elasticsearch.http.response.document.delete;

import static org.slf4j.LoggerFactory.getLogger;
import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.slf4j.Logger;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import io.netty.buffer.ByteBuf;
import rx.Observable;

public class DeleteResponseParser {

    private static final Logger logger = getLogger(DeleteResponseParser.class);

    public static Observable<DeleteResponse> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }

    private static DeleteResponse doParse(BytesReference bytesReference) {
        try {
            XContentParser parser = XContentHelper.createParser(bytesReference);

            DeleteResponse.DeleteResponseBuilder builder = DeleteResponse.builder();
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
                        case "found":
                            builder.found(parser.booleanValue());
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
