package org.elasticsearch.action.admin.indices.validate.query;

import java.util.List;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * @author olivier bourgain
 */
public class ValidateRequestAccessor {

    public static ValidateQueryResponse build(boolean valid, List<QueryExplanation> queryExplanations, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        return new ValidateQueryResponse(valid, queryExplanations, totalShards, successfulShards, failedShards, shardFailures);
    }

    public static BytesReference getSource(ValidateQueryRequest validateQueryRequest) {
        return validateQueryRequest.source();
    }

}
