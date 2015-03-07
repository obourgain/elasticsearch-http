package com.github.obourgain.elasticsearch.http.response.admin.indices.settings.get;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class UpdateSettingsResponse {

    private boolean acknowledged;

    public static Observable<UpdateSettingsResponse> parse(ByteBuf content) {
        return Observable.just(new UpdateSettingsResponse().doParse(new ByteBufBytesReference(content)));
    }

    private UpdateSettingsResponse doParse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("acknowledged".equals(currentFieldName)) {
                        acknowledged = parser.booleanValue();
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
