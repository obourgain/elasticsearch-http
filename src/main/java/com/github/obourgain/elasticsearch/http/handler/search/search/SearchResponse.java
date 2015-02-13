package com.github.obourgain.elasticsearch.http.handler.search.search;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Hits;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;
import com.github.obourgain.elasticsearch.http.response.parser.ShardParser;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Builder;
import rx.Observable;

@Builder
@Getter
public class SearchResponse {

    private String scrollId;
    private Shards shards;
    private Hits hits;
    private long tookInMillis;
    private boolean timedOut;
    private boolean terminatedEarly;
    private byte[] aggregations;

    public static Observable<SearchResponse> parse(ByteBuf byteBuf) {
        return Observable.just(doParse(new ByteBufBytesReference(byteBuf)));
    }

    protected static SearchResponse doParse(BytesReference bytes) {
        try {
            XContentParser parser = XContentHelper.createParser(bytes);

            SearchResponse.SearchResponseBuilder builder = builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("took".equals(currentFieldName)) {
                        builder.tookInMillis(parser.longValue());
                    } else if ("timed_out".equals(currentFieldName)) {
                        builder.timedOut(parser.booleanValue());
                    } else if ("_scroll_id".equals(currentFieldName)) {
                        builder.scrollId(parser.text());
                    } else if ("terminated_early".equals(currentFieldName)) {
                        builder.terminatedEarly(parser.booleanValue());
                    } else {
                        throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("_shards".equals(currentFieldName)) {
                        builder.shards(ShardParser.parseInner(parser));
                    } else if ("hits".equals(currentFieldName)) {
                        builder.hits(Hits.parse(parser));
                    } else if("aggregations".equals(currentFieldName)) {
                        XContentBuilder docBuilder = XContentFactory.contentBuilder(XContentType.JSON);
                        docBuilder.copyCurrentStructure(parser);
                        builder.aggregations(docBuilder.bytes().array());
                    }
                }
                // TODO shard failures
                // TODO facets ? maybe not
                // TODO suggests
            }
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
