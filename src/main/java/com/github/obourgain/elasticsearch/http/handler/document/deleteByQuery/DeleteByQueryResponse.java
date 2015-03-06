package com.github.obourgain.elasticsearch.http.handler.document.deleteByQuery;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Indices;
import com.github.obourgain.elasticsearch.http.response.parser.IndicesParser;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class DeleteByQueryResponse {

    private Indices indices;

    protected static Observable<DeleteByQueryResponse> parse(ByteBuf content) {
        return Observable.just(new DeleteByQueryResponse().doParse(new ByteBufBytesReference(content)));
    }

    protected DeleteByQueryResponse doParse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("_indices".equals(currentFieldName)) {
                        indices = IndicesParser.parse(parser);
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
