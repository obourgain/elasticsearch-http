package org.elasticsearch.action.admin.indices.open;

public class OpenIndexRequestAccessor {
    public static String[] indices(OpenIndexRequest request) {
        return request.indices();
    }
}
