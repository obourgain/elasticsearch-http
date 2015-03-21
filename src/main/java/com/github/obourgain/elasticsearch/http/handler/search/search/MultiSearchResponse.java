package com.github.obourgain.elasticsearch.http.handler.search.search;

import static org.elasticsearch.common.xcontent.XContentParser.Token.START_ARRAY;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class MultiSearchResponse {

    private List<SearchResponse> responses = new ArrayList<>();

    public static Observable<MultiSearchResponse> parse(ByteBuf byteBuf) {
        return Observable.just(new MultiSearchResponse().parse(new ByteBufBytesReference(byteBuf)));
    }

    protected MultiSearchResponse parse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            return doParse(parser);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected MultiSearchResponse doParse(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("responses".equals(currentFieldName)) {
                    parseResponses(parser);
                }
            }
        }
        return this;
    }

    protected void parseResponses(XContentParser parser) throws IOException {
        assert parser.currentToken() == START_ARRAY : "expected a START_ARRAY token but was " + parser.currentToken();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                responses.add(new SearchResponse().parse(parser));
            } else {
                throw new IllegalStateException("Parse failure, expected a START_OBJECT, got " + parser.currentToken() + " " + parser.currentName());
            }
        }
    }
}
