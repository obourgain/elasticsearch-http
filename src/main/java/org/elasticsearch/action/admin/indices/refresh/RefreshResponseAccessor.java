package org.elasticsearch.action.admin.indices.refresh;

import java.util.List;
import org.elasticsearch.action.ShardOperationFailedException;

/**
 * @author olivier bourgain
 */
public class RefreshResponseAccessor {

    public static RefreshResponse create(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        return new RefreshResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

}
