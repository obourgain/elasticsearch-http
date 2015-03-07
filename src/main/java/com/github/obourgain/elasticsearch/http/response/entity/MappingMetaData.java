package com.github.obourgain.elasticsearch.http.response.entity;

import java.io.IOException;
import java.util.Map;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import lombok.Getter;

@Getter
public class MappingMetaData {

    private String name;
    private byte[] source;

    public MappingMetaData parse(XContentParser parser) {
        try {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    name = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    source = copySource(parser);
                }
            }
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] copySource(XContentParser parser) {
        try(XContentBuilder docBuilder = XContentFactory.contentBuilder(XContentType.JSON)) {
            docBuilder.copyCurrentStructure(parser);
            return docBuilder.bytes().toBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Object> getAsMap() {
        return XContentHelper.convertToMap(source, 0, source.length, true).v2();
    }

    @Override
    public String toString() {
        // TODO this should be json to be easier to use ?
        return "MappingMetaData{" +
                "name='" + name + '\'' +
                ", source=" + new String(source) +
                '}';
    }
}
