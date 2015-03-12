package com.github.obourgain.elasticsearch.http.handler.search.suggest;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import com.github.obourgain.elasticsearch.http.response.entity.suggest.Suggestions;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class SuggestResponse {

    private Suggestions suggestions = new Suggestions();
    private Shards shards;

    public static Observable<SuggestResponse> parse(ByteBuf byteBuf) {
        ByteBufBytesReference bytesReference = new ByteBufBytesReference(byteBuf);
        System.out.println(bytesReference.toUtf8());
        return Observable.just(new SuggestResponse().parse(bytesReference));
    }

    protected SuggestResponse parse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != END_OBJECT) {
                if (token == FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == START_OBJECT) {
                    if ("_shards".equals(currentFieldName)) {
                        shards = new Shards().parse(parser);
                    } else if(currentFieldName != null) {
                        try (XContentBuilder docBuilder = XContentFactory.contentBuilder(XContentType.JSON)) {
                            suggestions.addRawSuggestion(currentFieldName, docBuilder);
                        }
                    }
                }
            }
            // TODO facets ? maybe not
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }
}
