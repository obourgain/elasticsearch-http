package com.github.obourgain.elasticsearch.http.response.admin.indices.mapping.get;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Map;
import org.elasticsearch.common.bytes.BytesArray;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;
import com.github.obourgain.elasticsearch.http.response.admin.indices.mapping.GetMappingsResponse;
import com.github.obourgain.elasticsearch.http.response.entity.MappingMetaData;

public class GetMappingsResponseTest {

    @Test
    public void should_parse_response() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/admin/indices/mappings/get/get_mapping_response.json");

        GetMappingsResponse response = new GetMappingsResponse().doParse(new BytesArray(json));

        Map<String, Map<String, MappingMetaData>> mappings = response.getMappings();

        assertThat(mappings).hasSize(1).containsKey("twitter");
        assertThat(mappings.get("twitter"))
                .hasSize(2)
                .containsKey("foo")
                .containsKey("tweet");
    }

    @Test
    public void should_parse_empty_mapping() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/admin/indices/mappings/get/one_empty_mapping.json");

        GetMappingsResponse response = new GetMappingsResponse().doParse(new BytesArray(json));

        Map<String, Map<String, MappingMetaData>> mappings = response.getMappings();

        assertThat(mappings).hasSize(1).containsKey("test_index_1");
        assertThat(mappings.get("test_index_1"))
                .hasSize(0);
    }

    @Test
    public void should_parse_empty_mapping_for_two_indices() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/admin/indices/mappings/get/two_empty_mappings.json");

        GetMappingsResponse response = new GetMappingsResponse().doParse(new BytesArray(json));

        Map<String, Map<String, MappingMetaData>> mappings = response.getMappings();

        assertThat(mappings).hasSize(2).containsKey("test_index_1").containsKey("test_index_2");
        assertThat(mappings.get("test_index_1")).hasSize(0);
        assertThat(mappings.get("test_index_2")).hasSize(0);
    }

    @Test
    public void should_parse_empty_and_nonempty_mappings_for_two_indices() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/admin/indices/mappings/get/mix_empty_nonempty_mappings.json");

        GetMappingsResponse response = new GetMappingsResponse().doParse(new BytesArray(json));

        Map<String, Map<String, MappingMetaData>> mappings = response.getMappings();

        assertThat(mappings).hasSize(2).containsKey("test_index_1").containsKey("twitter");
        assertThat(mappings.get("test_index_1")).hasSize(0);
        assertThat(mappings.get("twitter")).hasSize(2);
    }

}
