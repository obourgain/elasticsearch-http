package org.elasticsearch.action.admin.indices.delete;

/**
 * @author olivier bourgain
 */
public class DeleteIndexResponseAccessor {

    public static DeleteIndexResponse create(boolean acknowledged) {
        return new DeleteIndexResponse(acknowledged);
    }

}
