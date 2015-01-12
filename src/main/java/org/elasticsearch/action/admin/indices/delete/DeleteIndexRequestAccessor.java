package org.elasticsearch.action.admin.indices.delete;

/**
 * @author olivier bourgain
 */
public class DeleteIndexRequestAccessor {

    public static String[] indices(DeleteIndexRequest request) {
        return request.indices();
    }

}
