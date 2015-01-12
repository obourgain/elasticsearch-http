package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;

import java.util.List;

/**
 * @author olivier bourgain
 */
public class GetIndexTemplatesResponseAccessor {

    public static GetIndexTemplatesResponse create(List<IndexTemplateMetaData> metaData) {
        return new GetIndexTemplatesResponse(metaData);
    }
}
