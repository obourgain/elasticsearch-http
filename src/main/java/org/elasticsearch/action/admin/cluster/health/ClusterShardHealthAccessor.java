package org.elasticsearch.action.admin.cluster.health;

public class ClusterShardHealthAccessor {

    public static ClusterShardHealth create(int shardId, String status, boolean primaryActive, int activeShards, int relocatingShards, int initializingShards, int unassignedShards) {
        ClusterShardHealth shardHealth = new ClusterShardHealth(shardId);
        shardHealth.status = ClusterHealthStatus.valueOf(status.toUpperCase());
        shardHealth.primaryActive = primaryActive;
        shardHealth.activeShards = activeShards;
        shardHealth.relocatingShards = relocatingShards;
        shardHealth.initializingShards = initializingShards;
        shardHealth.unassignedShards = unassignedShards;
        return shardHealth;
    }
}
