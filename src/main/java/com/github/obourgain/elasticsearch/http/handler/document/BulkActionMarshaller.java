package com.github.obourgain.elasticsearch.http.handler.document;

import java.io.IOException;
import java.util.Iterator;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.VersionType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;

public class BulkActionMarshaller {

    @VisibleForTesting protected final Iterator<ActionRequest> requests;
    @VisibleForTesting protected byte[] nextRequestBody = null;

    public BulkActionMarshaller(BulkRequest request) {
        this.requests = request.requests().iterator();
    }

    public BulkActionMarshaller(Iterator<ActionRequest> requests) {
        this.requests = requests;
    }

    @Nullable
    public byte[] next() {
        if(nextRequestBody != null) {
            byte[] toReturn = nextRequestBody;
            nextRequestBody = null;
            return toReturn;
        }
        return requests.hasNext() ? marshall(requests.next()) : null;
    }

    private byte[] marshall(ActionRequest actionRequest) {
        try {
            return doMarshall(actionRequest);
        } catch (IOException e) {
            throw new RuntimeException("unable to marshall " + actionRequest, e);
        }
    }

    @VisibleForTesting
    protected byte[] doMarshall(ActionRequest actionRequest) throws IOException {
        String command;
        if (actionRequest instanceof IndexRequest) {
            IndexRequest indexRequest = (IndexRequest) actionRequest;
            command = buildIndexCommand(indexRequest);
            nextRequestBody = indexRequest.source().toBytes();

        } else if (actionRequest instanceof DeleteRequest) {
            DeleteRequest deleteRequest = (DeleteRequest) actionRequest;
            command = buildDeleteCommand(deleteRequest);

        } else if (actionRequest instanceof UpdateRequest) {
            UpdateRequest updateRequest = (UpdateRequest) actionRequest;
            command = buildUpdateCommand(updateRequest);

            String requestBody = UpdateHelper.buildRequestBody(updateRequest);
            nextRequestBody = requestBody.getBytes(Charsets.UTF_8);
        } else {
            throw new IllegalArgumentException("action type " + actionRequest.getClass().getName() + " not supported");
        }
        return command.getBytes(Charsets.UTF_8);
    }

    private String buildIndexCommand(IndexRequest indexRequest) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        addCommonOptions(builder, "index", indexRequest.index(), indexRequest.type(), indexRequest.id(),
                indexRequest.version(), indexRequest.versionType(), indexRequest.routing(),
                indexRequest.consistencyLevel(), indexRequest.refresh(), indexRequest.replicationType());
        String parent = indexRequest.parent();
        String timestamp = indexRequest.timestamp();
        long ttl = indexRequest.ttl();
        if (parent != null) {
            builder.field("_parent", parent);
        }
        if (timestamp != null) {
            builder.field("_timestamp", timestamp);
        }
        if (ttl != -1) {
            builder.field("_ttl", ttl);
        }
        if (indexRequest.opType() != null) {
            builder.field("op_type", indexRequest.opType().toString().toLowerCase());
        }
        if (indexRequest.parent() != null) {
            builder.field("op_type", indexRequest.parent());
        }
        return builder.string();
    }

    private String buildDeleteCommand(DeleteRequest deleteRequest) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        addCommonOptions(builder, "delete", deleteRequest.index(), deleteRequest.type(), deleteRequest.id(),
                deleteRequest.version(), deleteRequest.versionType(), deleteRequest.routing(),
                deleteRequest.consistencyLevel(), deleteRequest.refresh(), deleteRequest.replicationType());
        return builder.string();
    }

    private String buildUpdateCommand(UpdateRequest updateRequest) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        addCommonOptions(builder, "update", updateRequest.index(), updateRequest.type(), updateRequest.id(),
                updateRequest.version(), updateRequest.versionType(), updateRequest.routing(),
                updateRequest.consistencyLevel(), updateRequest.refresh(), updateRequest.replicationType());

        String timestamp = updateRequest.doc() != null ? updateRequest.doc().timestamp() : null;
        long ttl = updateRequest.doc() != null ? updateRequest.doc().ttl() : -1;

        if (timestamp != null) {
            builder.field("_timestamp", timestamp);
        }
        if (ttl != -1) {
            builder.field("_ttl", ttl);
        }
        if (updateRequest.retryOnConflict() != -1) {
            builder.field("_retry_on_conflict", updateRequest.retryOnConflict());
        }
        if (updateRequest.fields() != null && updateRequest.fields().length > 0) {
            builder.field("_fields", Strings.arrayToCommaDelimitedString(updateRequest.fields()));
        }
        if (updateRequest.doc() != null && updateRequest.doc().parent() != null) {
            builder.field("_parent", updateRequest.doc().parent());
        } else if (updateRequest.routing() != null) {
            builder.field("_parent", updateRequest.routing());
        }
        builder.bytes().array();
        return builder.string();
    }


    private void addCommonOptions(XContentBuilder builder, String command, String index, String type, String id,
                                  @Nullable long version, VersionType versionType, // may be Versions.MATCH_ANY and null
                                  @Nullable String routing,
                                  @Nullable WriteConsistencyLevel consistencyLevel,
                                  boolean refresh,
                                  @Nullable ReplicationType replicationType

    ) throws IOException {
        builder.startObject(command)
                .field("_index", index)
                .field("_type", type)
                .field("_id", id);
        if (version != Versions.MATCH_ANY) {
            builder.field("_version", version);
            builder.field("_version_type", versionType.toString().toLowerCase());
        }
        if (routing != null) {
            builder.field("_routing", routing);
        }
        if (consistencyLevel != WriteConsistencyLevel.DEFAULT) {
            builder.field("_consistency", consistencyLevel.toString().toLowerCase());
        }
        if (refresh) {
            builder.field("_refresh", true);
        }
        if (replicationType != null) {
            builder.field("replication", replicationType.toString().toLowerCase());
        }
    }

}
