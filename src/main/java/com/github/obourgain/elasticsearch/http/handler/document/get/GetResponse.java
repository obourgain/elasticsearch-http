package com.github.obourgain.elasticsearch.http.handler.document.get;

import java.io.IOException;
import java.util.Map;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.get.GetField;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.parser.FieldsParser;
import com.github.obourgain.elasticsearch.http.response.parser.SourceParser;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class GetResponse {

    private String index;
    private String type;
    private String id;
    private long version;
    private boolean found;
    private Map<String, Object> source;
    private Map<String, GetField> fields;

    public static Observable<GetResponse> parse(ByteBuf content) {
        return Observable.just(new GetResponse().doParse(new ByteBufBytesReference(content)));
    }

    private GetResponse doParse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
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
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
