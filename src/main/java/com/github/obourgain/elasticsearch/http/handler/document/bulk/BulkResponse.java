package com.github.obourgain.elasticsearch.http.handler.document.bulk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.util.VisibleForTesting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class BulkResponse {

    private long took;
    private boolean errors;
    private List<BulkItem> items;

    public static Observable<BulkResponse> parse(ByteBuf content) {
        return Observable.just(new BulkResponse().doParse(new ByteBufBytesReference(content)));
    }

    @VisibleForTesting
    protected BulkResponse doParse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            List<BulkItem> items = new ArrayList<>();

            this.items = items;
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("took".equals(currentFieldName)) {
                        took = parser.longValue();
                    } else if ("errors".equals(currentFieldName)) {
                        errors = parser.booleanValue();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    parser.nextToken();
                    if ("items".equals(currentFieldName)) {
                        while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
                            BulkItem bulkItem = new BulkItem().parse(parser);
                            items.add(bulkItem);
                        }
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
