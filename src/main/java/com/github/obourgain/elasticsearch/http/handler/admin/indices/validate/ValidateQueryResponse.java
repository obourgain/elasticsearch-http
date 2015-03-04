package com.github.obourgain.elasticsearch.http.handler.admin.indices.validate;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.ShardFailure;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import com.github.obourgain.elasticsearch.http.response.parser.ShardParser;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class ValidateQueryResponse {

    private Shards shards;
    private boolean valid;
    private List<ShardFailure> shardFailures = Collections.emptyList();

    public static Observable<ValidateQueryResponse> parse(ByteBuf content) {
        return Observable.just(new ValidateQueryResponse().parse(new ByteBufBytesReference(content)));
    }

    protected ValidateQueryResponse parse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("valid".equals(currentFieldName)) {
                        valid = parser.booleanValue();
                    } else {
                        throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("failures".equals(currentFieldName)) {
                        shardFailures = ShardFailure.parse(parser);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("_shards".equals(currentFieldName)) {
                        shards = ShardParser.parseInner(parser);
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
