package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import java.util.Collections;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.admin.indices.mapping.GetMappingsResponse;
import com.github.obourgain.elasticsearch.http.response.entity.MappingMetaData;

public class GetMappingsActionHandlerTest extends AbstractTest {

    @Test
    public void should_get_empty_mappings() throws Exception {
        createIndex("test_index_1", "test_index_2", "test_index_3");

        GetMappingsResponse response = httpClient.admin().indices().getMappings(new GetMappingsRequest().indices("test_index_1", "test_index_2")).get();

        Assertions.assertThat(response.getMappings()).hasSize(2)
                .containsKey("test_index_1")
                .containsKey("test_index_2");
    }

    @Test
    public void should_get_random_mapping() throws Exception {
        createIndex("test_index_1", "test_index_2", "test_index_3");
        transportClient.admin().indices().preparePutMapping("test_index_1", "test_index_2").setSource(Collections.singletonMap("twitter", Collections.emptyMap()));

        transportClient.index(Requests.indexRequest().index("test_index_1").type(THE_TYPE).id(THE_ID).refresh(true)
                        .source(XContentFactory.jsonBuilder().startObject()
                                .field("the_string_field", "the_string_value")
                                .field("the_integer_field", 42)
                                .field("the_boolean_field", true)
                                .field("the_long_array_field", new Long[]{42L, 53L})
                                .endObject())
        ).actionGet();
        transportClient.index(Requests.indexRequest().index("test_index_2").type(THE_TYPE).id(THE_ID).refresh(true)
                        .source(XContentFactory.jsonBuilder().startObject()
                                .field("the_string_field", "the_string_value")
                                .field("the_integer_field", 42)
                                .field("the_boolean_field", true)
                                .field("the_long_array_field", new Long[]{42L, 53L})
                                .endObject())
        ).actionGet();

        GetMappingsResponse response = httpClient.admin().indices().getMappings(new GetMappingsRequest().indices("test_index_1", "test_index_2")).get();

        Assertions.assertThat(response.getMappings())
                .hasSize(2)
                .containsKey("test_index_1")
                .containsKey("test_index_2");

        Map<String, MappingMetaData> test_index_1 = response.getMappings().get("test_index_1");
        System.out.println(test_index_1);
        Assertions.assertThat(test_index_1).isNotEmpty();
        Assertions.assertThat(response.getMappings().get("test_index_2")).isNotEmpty();
    }
}