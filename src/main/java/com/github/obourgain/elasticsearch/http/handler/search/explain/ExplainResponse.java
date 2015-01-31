package com.github.obourgain.elasticsearch.http.handler.search.explain;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Explanation;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.experimental.Builder;
import rx.Observable;

@Builder
@Getter
public class ExplainResponse {

    private String index;
    private String type;
    private String id;
    private boolean matched;
    private Explanation explanation;

    public static Observable<ExplainResponse> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }

    protected static ExplainResponse doParse(BytesReference bytesReference) {
        try {
            XContentParser parser = XContentHelper.createParser(bytesReference);

            ExplainResponseBuilder builder = builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("_index".equals(currentFieldName)) {
                        builder.index(parser.text());
                    } else if ("_type".equals(currentFieldName)) {
                        builder.type(parser.text());
                    } else if ("_id".equals(currentFieldName)) {
                        builder.id(parser.text());
                    } else if ("matched".equals(currentFieldName)) {
                        builder.matched(parser.booleanValue());
                    } else {
                        throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("explanation".equals(currentFieldName)) {
                        builder.explanation(Explanation.parseExplanation(parser));
                    }
                }
            }
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
