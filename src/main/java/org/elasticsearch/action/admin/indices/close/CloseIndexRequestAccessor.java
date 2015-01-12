package org.elasticsearch.action.admin.indices.close;

public class CloseIndexRequestAccessor {
    public static String[] indices(CloseIndexRequest request) {
        return request.indices();
    }
}
