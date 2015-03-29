package com.github.obourgain.elasticsearch.http.handler.search.multipercolate;

import static org.elasticsearch.common.lucene.uid.Versions.MATCH_ANY;
import java.io.IOException;
import java.util.List;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.VersionType;
import com.google.common.base.Charsets;
import rx.Observable;
import rx.functions.Func1;

public class PercolateRequestsMarshaller {

    private static final BytesReference LINE_BREAK = new BytesArray("\n".getBytes(Charsets.US_ASCII));
    public static final BytesArray EMPTY_JSON_OBJECT = new BytesArray("{}".getBytes());

    public static Observable<BytesReference> lazyConvertToBytes(final List<PercolateRequest> requests) {
        return Observable.from(requests)
                .flatMap(new Func1<PercolateRequest, Observable<BytesReference>>() {
                    @Override
                    public Observable<BytesReference> call(PercolateRequest request) {
                        try {
                            return Observable.just(writeHeader(request), LINE_BREAK, writeDoc(request), LINE_BREAK);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    private static BytesReference writeHeader(PercolateRequest request) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder().startObject()) {
            if(request.onlyCount()) {
                builder.startObject("count");
            } else {
                builder.startObject("percolate");
            }

            GetRequest getRequest = request.getRequest();
            if(getRequest != null) {
                builder.field("index", getRequest.index());
                builder.field("type", getRequest.type());
                builder.field("id", getRequest.id());
                if(getRequest.refresh()) {
                    builder.field("refresh", getRequest.refresh());
                }
                if(getRequest.realtime()) {
                    builder.field("realtime", getRequest.realtime());
                }
                if(getRequest.preference() != null) {
                    builder.field("preference", request.preference());
                }
                if(getRequest.routing() != null) {
                    builder.field("routing", request.routing());
                }
                if(getRequest.version() != MATCH_ANY) {
                    builder.field("version", getRequest.version());
                }
                if(getRequest.versionType() != VersionType.INTERNAL) {
                    builder.field("version_type", getRequest.versionType());
                }

                if(request.preference() != null) {
                    builder.field("percolate_preference", request.preference());
                }
                if(request.routing() != null) {
                    builder.field("percolate_routing", request.routing());
                }

                if(request.indices() != null && request.indices().length != 0) {
                    builder.field("percolate_index", Strings.arrayToCommaDelimitedString(request.indices()));
                }
                if(request.documentType() != null) {
                    builder.field("percolate_type", request.documentType());
                }
            } else {
                if(request.preference() != null) {
                    builder.field("preference", request.preference());
                }
                if(request.routing() != null) {
                    builder.field("routing", request.routing());
                }
                if(request.indices() != null && request.indices().length != 0) {
                    builder.field("index", Strings.arrayToCommaDelimitedString(request.indices()));
                }
                if(request.documentType() != null) {
                    builder.field("type", request.documentType());
                }
            }

            // TODO add a flag to disable ?
            // needs https://github.com/elastic/elasticsearch/pull/10307 to be merged
//            IndicesOptions indicesOptions = request.indicesOptions();
//            if(indicesOptions.expandWildcardsClosed() & indicesOptions.expandWildcardsOpen()) {
//                builder.field("expand_wildcards", "open,closed");
//            } else if(indicesOptions.expandWildcardsClosed()) {
//                builder.field("expand_wildcards", "closed");
//            } else if(indicesOptions.expandWildcardsOpen()) {
//                builder.field("expand_wildcards", "open");
//            }

            builder.endObject();
            builder.endObject();
            return builder.bytes();
        }
    }

    private static BytesReference writeDoc(PercolateRequest request) {
        return request.source() != null ? request.source() : EMPTY_JSON_OBJECT;
    }
}
