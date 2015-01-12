package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.common.settings.Settings;

public class UpdateSettingsRequestAccessor {

    public static String[] indices(UpdateSettingsRequest request) {
        return request.indices();
    }

    public static Settings settings(UpdateSettingsRequest request) {
        return request.settings();
    }
}
