package com.github.obourgain.elasticsearch.http.handler.document.multiget;

import static org.elasticsearch.common.xcontent.XContentParser.Token.START_ARRAY;
import java.io.IOException;
import java.util.List;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.handler.document.get.GetResponse;
import io.netty.buffer.ByteBuf;
import rx.Observable;

public class MultiGetResponse {

    private Eithers<MultiGetResponseError, GetResponse> docs = new Eithers<>();

    public List<MultiGetResponseError> errors() {
        return docs.lefts();
    }

    public List<GetResponse> documents() {
        return docs.rights();
    }

    public Eithers<MultiGetResponseError, GetResponse> all() {
        return docs;
    }

    protected static Observable<MultiGetResponse> parse(ByteBuf content) {
        return Observable.just(new MultiGetResponse().doParse(new ByteBufBytesReference(content)));
    }

    protected MultiGetResponse doParse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    assert currentFieldName != null;
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("docs".equals(currentFieldName)) {
                        parseDocs(parser);
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected MultiGetResponse parseDocs(XContentParser parser) throws IOException {
        assert parser.currentToken() == START_ARRAY : "expected a START_ARRAY token but was " + parser.currentToken();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                assert currentFieldName != null;
            } else if (token == XContentParser.Token.START_OBJECT) {
                docs.add(GetResponse.doParse(parser));
            }
        }
        return this;
    }
}
