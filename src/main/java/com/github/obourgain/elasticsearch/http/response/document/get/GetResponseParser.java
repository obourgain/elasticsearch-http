package com.github.obourgain.elasticsearch.http.response.document.get;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.parser.FieldsParser;
import com.github.obourgain.elasticsearch.http.response.parser.SourceParser;
import io.netty.buffer.ByteBuf;
import rx.Observable;

public class GetResponseParser {

    public static Observable<GetResponse> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }

    private static GetResponse doParse(BytesReference bytesReference) {
        try {
            XContentParser parser = XContentHelper.createParser(bytesReference);

            GetResponse.GetResponseBuilder builder = GetResponse.builder();
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
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("_source".equals(currentFieldName)) {
                        parser.nextToken();
                        builder.source(SourceParser.source(parser));
                    } else if ("fields".equals(currentFieldName)) {
                        parser.nextToken();
                        builder.fields(FieldsParser.fields(parser));
                    }
                }
            }
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
