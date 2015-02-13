package com.github.obourgain.elasticsearch.http.response.admin.indices.getaliases;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.FIELD_NAME;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.assertj.core.util.VisibleForTesting;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Builder;
import rx.Observable;

@Getter
@Builder
public class GetAliasesResponse {

    private ImmutableOpenMap<String, List<AliasMetaData>> aliases = ImmutableOpenMap.of();

    public static Observable<GetAliasesResponse> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }

    @VisibleForTesting
    protected static GetAliasesResponse doParse(BytesReference body) {
        try {
            XContentParser parser = XContentHelper.createParser(body);

            ListMultimap<String, AliasMetaData> metaDatas = ArrayListMultimap.create();

            GetAliasesResponseBuilder builder = builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == START_OBJECT) {
                    if (currentFieldName != null) { // we are at an index metadata start
                        List<AliasMetaData> aliasMetaDatas = parseAliases(parser);
                        metaDatas.putAll(currentFieldName, aliasMetaDatas);
                    }
                }
            }
            Map<String, List<AliasMetaData>> map = Multimaps.asMap(metaDatas);
            builder.aliases(ImmutableOpenMap.<String, List<AliasMetaData>>builder().putAll(map).build());
            return builder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    protected static List<AliasMetaData> parseAliases(XContentParser parser) {
        assert parser.currentToken() == START_OBJECT;
        try {
            List<AliasMetaData> result = new ArrayList<>();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == START_OBJECT) {
                    if ("aliases".equals(currentFieldName)) {
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            AliasMetaData metaData = parseAlias(parser);
                            result.add(metaData);
                        }
                    }
                }
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    protected static AliasMetaData parseAlias(XContentParser parser) {
        assert parser.currentToken() == FIELD_NAME : "expected a FIELD_NAME token but was " + parser.currentToken();
        try {
            return AliasMetaData.Builder.fromXContent(parser);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
