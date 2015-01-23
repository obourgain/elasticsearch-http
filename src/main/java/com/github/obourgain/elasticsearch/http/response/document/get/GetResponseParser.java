package com.github.obourgain.elasticsearch.http.response.document.get;

import static org.slf4j.LoggerFactory.getLogger;
import java.io.IOException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.slf4j.Logger;
import com.github.obourgain.elasticsearch.http.buffer.ByteBufBytesReference;
import com.github.obourgain.elasticsearch.http.response.parser.FieldsParser;
import com.github.obourgain.elasticsearch.http.response.parser.SourceParser;
import com.ning.http.client.Response;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

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
        try {
            XContentParser parser = XContentHelper.createParser(body, 0, body.length);

            GetResponse.GetResponseBuilder builder = GetResponse.builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    assert currentFieldName != null;
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

    private static GetResponse doParse(BytesReference bytesReference) {
        try {
            XContentParser parser = XContentHelper.createParser(bytesReference);

            GetResponse.GetResponseBuilder builder = GetResponse.builder();
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    assert currentFieldName != null;
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
        rx.Observable<GetResponse> map = response.getContent().map(new Func1<ByteBuf, GetResponse>() {
            @Override
            public GetResponse call(ByteBuf b) {
                return doParse(new ByteBufBytesReference(b));
            }
        });
        return map.single();
    }

    public static Observable<GetResponse> parse(ByteBuf content) {
        return Observable.just(doParse(new ByteBufBytesReference(content)));
    }
//    public static GetResponse parse(ByteBuf content) {
//        return doParse(new ByteBufBytesReference(content));
//    }
}
