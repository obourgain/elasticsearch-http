package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.common.settings.Settings;

public class ClusterUpdateSettingsResponseAccessor {

    public static ClusterUpdateSettingsResponse create(boolean acknowledged, Settings transients, Settings persistents) {
        return new ClusterUpdateSettingsResponse(acknowledged, transients, persistents);
    }
}
