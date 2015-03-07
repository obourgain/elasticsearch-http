package com.github.obourgain.elasticsearch.http.response.admin.indices.mapping;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;
import static org.elasticsearch.common.xcontent.XContentParser.Token.START_OBJECT;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.entity.MappingMetaData;
import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public class GetMappingsResponse {

    private Map<String, Map<String, MappingMetaData>> mappings = new HashMap<>();

    public GetMappingsResponse parse(ByteBuf content) {
        return doParse(new ByteBufBytesReference(content));
    }

    public GetMappingsResponse doParse(BytesReference content) {
        try (XContentParser parser = XContentHelper.createParser(content)) {
            parser.nextToken();
            return doParse(parser);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected GetMappingsResponse doParse(XContentParser parser) throws IOException {
        assert parser.currentToken() == START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                Map<String, MappingMetaData> mappingsForIndex = parseIndex(parser);
                // skip the end token of the index's mapping because due to the return in the loop of parseIndex it is not consumed
                assert parser.currentToken() == END_OBJECT : "expected a END_OBJECT token but was " + parser.currentToken();
                parser.nextToken();
                mappings.put(currentFieldName, mappingsForIndex);
            }
        }
        return this;
    }

    protected Map<String, MappingMetaData> parseIndex(XContentParser parser) throws IOException {
        assert parser.currentToken() == START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if(currentFieldName != null && currentFieldName.equals("mappings")) {
                    return parseMappings(parser);
                }
            }
        }
        throw new IllegalStateException("'mappings' field not found");
    }

    protected Map<String, MappingMetaData> parseMappings(XContentParser parser) throws IOException {
        assert parser.currentToken() == START_OBJECT : "expected a START_OBJECT token but was " + parser.currentToken();

        Map<String, MappingMetaData> mappingsForIndex = new HashMap<>();

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                MappingMetaData metadata = new MappingMetaData().parse(parser);
                mappingsForIndex.put(currentFieldName, metadata);
            }
        }
        return mappingsForIndex;
    }
}
