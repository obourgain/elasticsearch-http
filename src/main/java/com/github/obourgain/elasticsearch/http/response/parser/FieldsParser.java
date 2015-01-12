package com.github.obourgain.elasticsearch.http.response.parser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.get.GetField;
import com.google.common.collect.ImmutableMap;

public class FieldsParser {

    public static Map<String, GetField> fields(XContentParser parser) {
        try {
            Map<String, Object> map = parser.mapOrdered();
            return toGetFieldMap(map);
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse source", e);
        }
    }

    private static Map<String, GetField> toGetFieldMap(@Nullable Map<String, Object> fieldsAsMap) {
        if (fieldsAsMap == null) {
            return ImmutableMap.of();
        }
        Map<String, GetField> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : fieldsAsMap.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof List) {
                result.put(name, new GetField(name, (List<Object>) value));
            } else {
                result.put(name, new GetField(name, Collections.singletonList(value)));
            }
        }
        return result;
    }
}
