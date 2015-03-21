package com.github.obourgain.elasticsearch.http.handler.search.search;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Hits;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import com.github.obourgain.elasticsearch.http.response.entity.aggs.Aggregations;
import com.github.obourgain.elasticsearch.http.response.entity.suggest.Suggestions;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class SearchResponse {

    private String scrollId;
    private Shards shards;
    private Hits hits;
    private long tookInMillis;
    private boolean timedOut;
    private boolean terminatedEarly;
    private Aggregations aggregations;
    private Suggestions suggestions;

    public static Observable<SearchResponse> parse(ByteBuf byteBuf) {
        return Observable.just(new SearchResponse().parse(new ByteBufBytesReference(byteBuf)));
    }

    protected SearchResponse parse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("took".equals(currentFieldName)) {
                        tookInMillis = parser.longValue();
                    } else if ("timed_out".equals(currentFieldName)) {
                        timedOut = parser.booleanValue();
                    } else if ("_scroll_id".equals(currentFieldName)) {
                        scrollId = parser.text();
                    } else if ("terminated_early".equals(currentFieldName)) {
                        terminatedEarly = parser.booleanValue();
                    } else {
                        throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("_shards".equals(currentFieldName)) {
                        shards = new Shards().parse(parser);
                    } else if ("hits".equals(currentFieldName)) {
                        hits = new Hits().parse(parser);
                    } else if ("suggest".equals(currentFieldName)) {
                        suggestions = Suggestions.parse(parser);
                    } else if ("aggregations".equals(currentFieldName)) {
                        aggregations = Aggregations.parse(parser);
                    }
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
