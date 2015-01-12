package org.elasticsearch.action.admin.indices.optimize;

import java.util.List;
import org.elasticsearch.action.ShardOperationFailedException;

public class OptimizeResponseAccessor {

    public static OptimizeResponse create(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        return new OptimizeResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

}
