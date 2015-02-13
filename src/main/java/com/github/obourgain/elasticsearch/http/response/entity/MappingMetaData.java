package com.github.obourgain.elasticsearch.http.response.entity;

import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Builder;
import rx.Observable;

@Getter
@Builder
public class MappingMetaData {

    public static Observable<MappingMetaData> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }

    private static MappingMetaData doParse(BytesReference bytesReference) {
        try {
            XContentParser parser = XContentHelper.createParser(bytesReference);
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
