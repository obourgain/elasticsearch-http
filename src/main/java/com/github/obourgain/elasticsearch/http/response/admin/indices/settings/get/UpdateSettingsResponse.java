package com.github.obourgain.elasticsearch.http.response.admin.indices.settings.get;

import java.io.IOException;
import java.util.Map;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.experimental.Builder;
import rx.Observable;

@Getter
@Builder
public class UpdateSettingsResponse {

    // TODO

    private Map<String, Map<String, MappingMetaData>> mappings;

    public static Observable<UpdateSettingsResponse> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }

    private static UpdateSettingsResponse doParse(BytesReference bytesReference) {
        try {
            XContentParser parser = XContentHelper.createParser(bytesReference);

            UpdateSettingsResponseBuilder builder = builder();
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
