package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;

public class ClusterStateResponseAccessor {

    public static ClusterStateResponse create(ClusterName clusterName, ClusterState clusterState) {
        return new ClusterStateResponse(clusterName, clusterState);
    }

}
