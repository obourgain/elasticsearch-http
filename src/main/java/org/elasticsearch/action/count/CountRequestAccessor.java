package org.elasticsearch.action.count;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.List;

/**
 * @author olivier bourgain
 */
public class CountRequestAccessor {

    /**
     * This exposes the package visible constructor {@link CountResponse#CountResponse(long, boolean, int, int, int, List)}.
     */
    public static CountResponse build(long count, boolean hasTerminatedEarly, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        return new CountResponse(count, hasTerminatedEarly, totalShards, successfulShards, failedShards, shardFailures);
    }

    public static BytesReference getSource(CountRequest countRequest) {
        return countRequest.source();
    }

    public static float getMinScore(CountRequest countRequest) {
        return countRequest.minScore();
    }

}
