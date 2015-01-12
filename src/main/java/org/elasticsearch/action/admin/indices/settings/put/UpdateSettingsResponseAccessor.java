package org.elasticsearch.action.admin.indices.settings.put;

public class UpdateSettingsResponseAccessor {

    public static UpdateSettingsResponse create(boolean acknowledged) {
        return new UpdateSettingsResponse(acknowledged);
    }

}
