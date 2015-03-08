package com.github.obourgain.elasticsearch.http.handler.search.explain;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Explanation;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class ExplainResponse {

    private String index;
    private String type;
    private String id;
    private boolean matched;
    private Explanation explanation;

    public static Observable<ExplainResponse> parse(ByteBuf content) {
        return Observable.just(new ExplainResponse().doParse(new ByteBufBytesReference(content)));
    }

    protected ExplainResponse doParse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("_index".equals(currentFieldName)) {
                        index = parser.text();
                    } else if ("_type".equals(currentFieldName)) {
                        type = parser.text();
                    } else if ("_id".equals(currentFieldName)) {
                        id = parser.text();
                    } else if ("matched".equals(currentFieldName)) {
                        matched = parser.booleanValue();
                    } else {
                        throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("explanation".equals(currentFieldName)) {
                        explanation = new Explanation().parse(parser);
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
