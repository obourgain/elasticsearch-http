package com.github.obourgain.elasticsearch.http.handler.search.exists;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.ShardFailure;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class ExistsResponse {

    private boolean exists;
    private List<ShardFailure> shardFailures = Collections.emptyList();

    public static Observable<ExistsResponse> parse(ByteBuf content) {
        return Observable.just(new ExistsResponse().parse(new ByteBufBytesReference(content)));
    }

    private ExistsResponse parse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("exists".equals(currentFieldName)) {
                        exists=parser.booleanValue();
                    } else {
                        throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if("failures".equals(currentFieldName)) {
                        shardFailures = ShardFailure.parse(parser);
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
