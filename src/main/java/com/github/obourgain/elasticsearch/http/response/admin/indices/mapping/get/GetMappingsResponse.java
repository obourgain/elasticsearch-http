package com.github.obourgain.elasticsearch.http.response.admin.indices.mapping.get;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.Hits;
import com.github.obourgain.elasticsearch.http.response.entity.aggs.Aggregations;
import com.github.obourgain.elasticsearch.http.response.parser.ShardParser;
import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Getter;
import rx.Observable;

@Getter
public class GetMappingsResponse {

    private Map<String, Map<String, MappingMetaData>> mappings = new HashMap<>();

//    protected static Observable<GetMappingsResponse> parse(ByteBuf content) {
//        return Observable.just(doParse(new ByteBufBytesReference(content)));
//    }

    public GetMappingsResponse doParse(ByteBuf content) {
        try (XContentParser parser = XContentHelper.createParser(new ByteBufBytesReference(content))) {
            // TODO
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
//                } else if (token.isValue()) {
//                    if ("took".equals(currentFieldName)) {
//                        builder.tookInMillis(parser.longValue());
//                    } else if ("timed_out".equals(currentFieldName)) {
//                        builder.timedOut(parser.booleanValue());
//                    } else if ("_scroll_id".equals(currentFieldName)) {
//                        builder.scrollId(parser.text());
//                    } else if ("terminated_early".equals(currentFieldName)) {
//                        builder.terminatedEarly(parser.booleanValue());
//                    } else {
//                        throw new IllegalStateException("unknown field " + currentFieldName);
//                    }
                } else if (token == XContentParser.Token.START_OBJECT) {

                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public GetMappingsResponse parseIndex(XContentParser parser) throws IOException {
            // TODO
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
//                } else if (token.isValue()) {
//                    if ("took".equals(currentFieldName)) {
//                        builder.tookInMillis(parser.longValue());
//                    } else if ("timed_out".equals(currentFieldName)) {
//                        builder.timedOut(parser.booleanValue());
//                    } else if ("_scroll_id".equals(currentFieldName)) {
//                        builder.scrollId(parser.text());
//                    } else if ("terminated_early".equals(currentFieldName)) {
//                        builder.terminatedEarly(parser.booleanValue());
//                    } else {
//                        throw new IllegalStateException("unknown field " + currentFieldName);
//                    }
                } else if (token == XContentParser.Token.START_OBJECT) {

                }
            }
            return this;
    }

    public GetMappingsResponse parseType(XContentParser parser) throws IOException {
            // TODO
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
//                } else if (token.isValue()) {
//                    if ("took".equals(currentFieldName)) {
//                        builder.tookInMillis(parser.longValue());
//                    } else if ("timed_out".equals(currentFieldName)) {
//                        builder.timedOut(parser.booleanValue());
//                    } else if ("_scroll_id".equals(currentFieldName)) {
//                        builder.scrollId(parser.text());
//                    } else if ("terminated_early".equals(currentFieldName)) {
//                        builder.terminatedEarly(parser.booleanValue());
//                    } else {
//                        throw new IllegalStateException("unknown field " + currentFieldName);
//                    }
                } else if (token == XContentParser.Token.START_OBJECT) {

                }
            }
            return this;
    }
}
