package org.elasticsearch.action.exists;

import java.util.List;
import org.elasticsearch.action.ShardOperationFailedException;

public class ExistsResponseAccessor {

    public static ExistsResponse create(boolean exists, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        return new ExistsResponse(exists, totalShards, successfulShards, failedShards, shardFailures);
    }

}
