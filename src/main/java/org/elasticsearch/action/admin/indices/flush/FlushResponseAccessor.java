package org.elasticsearch.action.admin.indices.flush;

import java.util.List;
import org.elasticsearch.action.ShardOperationFailedException;

public class FlushResponseAccessor {

    public static FlushResponse create(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        return new FlushResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

}
