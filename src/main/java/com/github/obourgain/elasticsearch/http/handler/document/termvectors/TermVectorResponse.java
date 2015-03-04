package com.github.obourgain.elasticsearch.http.handler.document.termvectors;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.TermVector;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class TermVectorResponse {

    private String index;
    private String type;
    private String id;
    private long version;
    private boolean found;
    private TermVector termVector;

    protected static Observable<TermVectorResponse> parse(ByteBuf content) {
        return Observable.just(new TermVectorResponse().parse(new ByteBufBytesReference(content)));
    }

    protected TermVectorResponse parse(BytesReference bytesReference) {
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
                            break;
                        case "found":
                            found = parser.booleanValue();
                            break;
                        default:
                            throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("term_vectors".equals(currentFieldName)) {
                        termVector = new TermVector().parse(parser);
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
