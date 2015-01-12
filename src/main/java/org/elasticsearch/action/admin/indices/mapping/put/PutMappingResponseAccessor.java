package org.elasticsearch.action.admin.indices.mapping.put;

public class PutMappingResponseAccessor {
    public static PutMappingResponse create(boolean acknowledged) {
        return new PutMappingResponse(acknowledged);
    }
}
