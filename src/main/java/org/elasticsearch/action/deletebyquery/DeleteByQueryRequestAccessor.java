package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.List;

/**
 * @author olivier bourgain
 */
public class DeleteByQueryRequestAccessor {

    public static DeleteByQueryResponse build() {
        return new DeleteByQueryResponse();
    }

    public static IndexDeleteByQueryResponse buildIndexResponse(String index, int successfulShards, int failedShards, List<ShardOperationFailedException> failures) {
        return new IndexDeleteByQueryResponse(index, successfulShards, failedShards, failures);
    }

    public static BytesReference getSource(DeleteByQueryRequest deleteByQueryRequest) {
        return deleteByQueryRequest.source();
    }

    public static String[] types(DeleteByQueryRequest deleteByQueryRequest) {
        return deleteByQueryRequest.types();
    }

}
