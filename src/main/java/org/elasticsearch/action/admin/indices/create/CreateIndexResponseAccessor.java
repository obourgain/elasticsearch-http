package org.elasticsearch.action.admin.indices.create;

/**
 * @author olivier bourgain
 */
public class CreateIndexResponseAccessor {

    public static CreateIndexResponse create(boolean acknowledged) {
        return new CreateIndexResponse(acknowledged);
    }
}
