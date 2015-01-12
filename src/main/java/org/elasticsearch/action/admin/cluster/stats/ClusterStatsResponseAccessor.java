package org.elasticsearch.action.admin.cluster.stats;

import java.io.IOException;
import org.elasticsearch.common.io.stream.StreamInput;

public class ClusterStatsResponseAccessor {

    public static ClusterStatsResponse create(StreamInput in) {
        ClusterStatsResponse clusterStatsNodeResponses = new ClusterStatsResponse();
        try {
            clusterStatsNodeResponses.readFrom(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return clusterStatsNodeResponses;
    }

}
