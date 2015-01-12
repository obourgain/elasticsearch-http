package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.common.settings.Settings;

public class ClusterUpdateSettingsRequestAccessor {
    public static Settings transientSettings(ClusterUpdateSettingsRequest request) {
        return request.transientSettings();
    }

    public static Settings persistentSettings(ClusterUpdateSettingsRequest request) {
        return request.persistentSettings();
    }
}
