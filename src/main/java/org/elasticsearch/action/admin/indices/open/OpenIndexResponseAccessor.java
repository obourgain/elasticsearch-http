package org.elasticsearch.action.admin.indices.open;

public class OpenIndexResponseAccessor {

    public static OpenIndexResponse create(boolean acknowledged) {
        return new OpenIndexResponse(acknowledged);
    }
}
