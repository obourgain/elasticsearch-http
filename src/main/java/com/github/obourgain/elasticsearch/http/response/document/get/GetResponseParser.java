package com.github.obourgain.elasticsearch.http.response.document.get;

import static org.slf4j.LoggerFactory.getLogger;
import java.io.IOException;
import org.elasticsearch.common.bytes.ChannelBufferBytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.slf4j.Logger;
import com.github.obourgain.elasticsearch.http.response.parser.FieldsParser;
import com.github.obourgain.elasticsearch.http.response.parser.SourceParser;
import com.ning.http.client.Response;
import io.netty.buffer.ByteBuf;
import rx.Observable;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;

public class GetResponseParser {

    private static final Logger logger = getLogger(GetResponseParser.class);

    public static GetResponse parse(Response response) {
        try {
            return doParse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static GetResponse doParse(Response response) throws IOException {
        byte[] body = response.getResponseBodyAsBytes();
        if (logger.isTraceEnabled()) {
            logger.trace("Parsing {}", new String(body, 0, body.length));
        }
        return doParse(body);
    }

    private static GetResponse doParse(byte[] body) {
        return doParse(body, 0, body.length);
    }

    private static GetResponse doParse(byte[] body, int start, int end) {
        try {
            XContentHelper.convertToJson(new ChannelBufferBytesReference())
            XContentParser parser = XContentHelper.createParser(body, start, end);

            GetResponse.GetResponseBuilder builder = GetResponse.builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    switch (currentFieldName) {
                        case "_index":
                            builder.index(parser.text());
                            break;
                        case "_type":
                            builder.type(parser.text());
                            break;
                        case "_id":
                            builder.id(parser.text());
                            break;
                        case "_version":
                            builder.version(parser.longValue());
                        case "found":
                            builder.found(parser.booleanValue());
                            break;
                        default:
                            throw new IllegalStateException("unknown field " + currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("_source".equals(currentFieldName)) {
                        parser.nextToken();
                        builder.source(SourceParser.source(parser));
                    } else if ("fields".equals(currentFieldName)) {
                        parser.nextToken();
                        builder.fields(FieldsParser.fields(parser));
                    }
                }
            }
            return builder.build();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Observable<GetResponse> parse(HttpClientResponse<ByteBuf> response) {
        rx.Observable<GetResponse> map = response.getContent().map(b -> doParse(b.array(), b.arrayOffset(), b.array().length));
        return map.single();
    }
}
