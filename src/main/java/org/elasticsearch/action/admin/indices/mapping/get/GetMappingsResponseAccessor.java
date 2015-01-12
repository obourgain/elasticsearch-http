package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;

public class GetMappingsResponseAccessor {

    public static GetMappingsResponse create(ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings) {
        return new GetMappingsResponse(mappings);
    }

}
