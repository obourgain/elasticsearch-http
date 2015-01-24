package com.github.obourgain.elasticsearch.http.response.document.deleteByQuery;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Indices;
import com.github.obourgain.elasticsearch.http.response.parser.IndicesParser;
import io.netty.buffer.ByteBuf;
import rx.Observable;

public class DeleteByQueryResponseParser {

    public static Observable<DeleteByQueryResponse> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }

    private static DeleteByQueryResponse doParse(BytesReference body) {
        try {
            XContentParser parser = XContentHelper.createParser(body);

            // TODO parse failures

            DeleteByQueryResponse.DeleteByQueryResponseBuilder builder = DeleteByQueryResponse.builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("_indices".equals(currentFieldName)) {
                        Indices indices = IndicesParser.parse(parser);
                        builder.indices(indices);
                    }
                }
            }
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
