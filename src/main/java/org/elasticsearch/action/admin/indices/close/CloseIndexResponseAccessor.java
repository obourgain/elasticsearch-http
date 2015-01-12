package org.elasticsearch.action.admin.indices.close;

public class CloseIndexResponseAccessor {

    public static CloseIndexResponse create(boolean acknowledged) {
        return new CloseIndexResponse(acknowledged);
    }
}
