package com.github.obourgain.elasticsearch.http.handler.search.multipercolate;

import static org.elasticsearch.common.xcontent.XContentParser.Token.START_ARRAY;
import java.io.IOException;
import java.util.List;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.handler.document.multiget.Eithers;
import com.github.obourgain.elasticsearch.http.handler.search.percolate.PercolateResponse;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class MultiPercolateResponse {
    private Eithers<MultiPercolateResponseError, PercolateResponse> responses = new Eithers<>();

    public List<MultiPercolateResponseError> errors() {
        return responses.lefts();
    }

    public List<PercolateResponse> percolated() {
        return responses.rights();
    }

    public Eithers<MultiPercolateResponseError, PercolateResponse> all() {
        return responses;
    }

    protected static Observable<MultiPercolateResponse> parse(ByteBuf content) {
        return Observable.just(new MultiPercolateResponse().doParse(new ByteBufBytesReference(content)));
    }

    protected MultiPercolateResponse doParse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    assert currentFieldName != null;
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("responses".equals(currentFieldName)) {
                        parseDocs(parser);
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected MultiPercolateResponse parseDocs(XContentParser parser) throws IOException {
        assert parser.currentToken() == START_ARRAY : "expected a START_ARRAY token but was " + parser.currentToken();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                assert currentFieldName != null;
            } else if (token == XContentParser.Token.START_OBJECT) {
                responses.add(PercolateResponse.doParse(parser));
            }
        }
        return this;
    }
}
