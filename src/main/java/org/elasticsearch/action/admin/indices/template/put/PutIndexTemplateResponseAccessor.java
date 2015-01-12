package org.elasticsearch.action.admin.indices.template.put;

/**
 * @author olivier bourgain
 */
public class PutIndexTemplateResponseAccessor {

    public static PutIndexTemplateResponse create(Boolean acknowledged) {
        return new PutIndexTemplateResponse(acknowledged);
    }
}
