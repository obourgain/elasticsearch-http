package com.github.obourgain.elasticsearch.http.handler.admin.indices.mapping.put;

import java.util.Map;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.MapEntry;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;

public class PutMappingActionHandlerTest extends AbstractTest {

    @Test
    public void should_put_mapping() throws Exception {
        String type = "test_type";
        PutMappingRequest request = new PutMappingRequest(THE_INDEX)
                .ignoreConflicts(true)
                .type(type)
                .source("{\n" +
                        "    \"properties\" : {\n" +
                        "        \"message\" : {\"type\" : \"string\", \"store\" : true }\n" +
                        "    }\n" +
                        "}");

        httpClient.admin().indices().putMapping(request).get();

        GetMappingsResponse getMappingsResponse = transportClient.admin().indices().getMappings(new GetMappingsRequest().indices(THE_INDEX)).actionGet();

        ImmutableOpenMap<String, MappingMetaData> mapping = getMappingsResponse.getMappings().get(THE_INDEX);

        Assertions.assertThat(mapping.containsKey(type)).isTrue();

        MappingMetaData mappingMetaData = mapping.get(type);
        Map<String, Object> map = mappingMetaData.sourceAsMap();

        Assertions.assertThat(map.containsKey("properties")).isTrue();

        Assertions.assertThat(map.get("properties")).isInstanceOf(Map.class);
        Map<String, Object> properties = (Map) map.get("properties");

        Assertions.assertThat(properties.get("message")).isInstanceOf(Map.class);

        Map<String, Object> message = (Map<String, Object>) properties.get("message");
        Assertions.assertThat(message).contains(MapEntry.entry("type", "string"), MapEntry.entry("store", true));
    }
}