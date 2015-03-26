package com.github.obourgain.elasticsearch.http.handler.document.get;

import java.io.IOException;
import java.util.Map;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.get.GetField;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.handler.document.multiget.Either;
import com.github.obourgain.elasticsearch.http.handler.document.multiget.MultiGetResponseError;
import com.github.obourgain.elasticsearch.http.response.parser.FieldsParser;
import com.github.obourgain.elasticsearch.http.response.parser.SourceParser;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;
import rx.Observable;

@Getter
@AllArgsConstructor
public class GetResponse {

    private String index;
    private String type;
    private String id;
    private long version;
    private boolean found;
    private Map<String, Object> source;
    private Map<String, GetField> fields;

    public static Observable<GetResponse> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }

    public static GetResponse doParse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            // here we are parsing a GetRequest response so we may only have a right because ES would have responded with 404
            // if the document doesn't exists
            return doParse(parser).right();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Either<MultiGetResponseError, GetResponse> doParse(XContentParser parser) throws IOException {
        String index = null;
        String type = null;
        String id = null;
        long version = 0;
        boolean found = false;
        Map<String, Object> source = null;
        Map<String, GetField> fields = null;
        String error = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                assert currentFieldName != null;
                switch (currentFieldName) {
                    case "_index":
                        index = parser.text();
                        break;
                    case "_type":
                        type = parser.text();
                        break;
                    case "_id":
                        id = parser.text();
                        break;
                    case "_version":
                        version = parser.longValue();
                    case "found":
                        found = parser.booleanValue();
                        break;
                    case "error":
                        error = parser.text();
                        break;
                    default:
                        throw new IllegalStateException("unknown field " + currentFieldName);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("_source".equals(currentFieldName)) {
                    parser.nextToken();
                    source = SourceParser.source(parser);
                } else if ("fields".equals(currentFieldName)) {
                    parser.nextToken();
                    fields = FieldsParser.fields(parser);
                }
            }
        }
        if(error == null) {
            return Either.right(new GetResponse(index, type, id, version, found, source, fields));
        } else {
            return Either.left(new MultiGetResponseError(index, type, id, error));
        }
    }
}
