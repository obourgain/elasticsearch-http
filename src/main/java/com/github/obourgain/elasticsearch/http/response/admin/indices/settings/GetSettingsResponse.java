package com.github.obourgain.elasticsearch.http.response.admin.indices.settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.google.common.base.Joiner;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import rx.Observable;

@Getter
public class GetSettingsResponse {

    private static final Joiner settingsJoiner = Joiner.on(".");

    private ImmutableOpenMap<String, Settings> indexToSettings = ImmutableOpenMap.of();

    public static Observable<GetSettingsResponse> parse(ByteBuf content) {
        return Observable.just(new GetSettingsResponse().doParse(new ByteBufBytesReference(content)));
    }

    private GetSettingsResponse doParse(BytesReference bytesReference) {
        try (XContentParser parser = XContentHelper.createParser(bytesReference)) {
            ImmutableOpenMap.Builder<String, Settings> builder = ImmutableOpenMap.builder();
            Map<String, Object> map = parser.map();

            for (String index : map.keySet()) {
                Map<String, Map<String, Object>> indexSettings = getAsNestedStringToMapMap(map, index);
                Map<String, Map<String, Object>> settingsAsMap = getAsNestedStringToMapMap(indexSettings, "settings");
                Map<String, String> flattenedSettings = flattenSettings(settingsAsMap);
                Settings settings = ImmutableSettings.builder().put(flattenedSettings).build();
                builder.put(index, settings);
            }
            this.indexToSettings = builder.build();
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * We get settings as nested objects whereas the expected result is to have dot separated flat path
     */
    private Map<String, String> flattenSettings(Map<String, ?> settingsAsMap) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, ?> entry : settingsAsMap.entrySet()) {
            if (entry.getValue() instanceof Map) {
                ArrayList<String> prefix = new ArrayList<>();
                prefix.add(entry.getKey());
                flattenSettings((Map<String, ?>) entry.getValue(), prefix, result);
            } else {
                result.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        return result;
    }

    private void flattenSettings(Map<String, ?> settingsAsMap, List<String> prefix, Map<String, String> output) {
        for (Map.Entry<String, ?> entry : settingsAsMap.entrySet()) {
            // copy to avoid sharing it
            List<String> copy = new ArrayList<>(prefix);
            copy.add(entry.getKey());
            if (entry.getValue() instanceof Map) {
                flattenSettings((Map<String, ?>) entry.getValue(), copy, output);
            } else {
                output.put(settingsJoiner.join(copy), String.valueOf(entry.getValue()));
            }
        }
    }

    @Nullable
    private Map<String, Map<String, Object>> getAsNestedStringToMapMap(Map map, String key) {
        // hide the unchecked cast under the carpet
        return (Map<String, Map<String, Object>>) getAs(map, key, Map.class);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private static <T> T getAs(Map<String, Object> map, String key, Class<T> type) {
        return (T) map.get(key);
    }

    @Override
    public String toString() {
        return "GetSettingsResponse{" +
                "indexToSettings=" + indexToSettings +
                '}';
    }
}
