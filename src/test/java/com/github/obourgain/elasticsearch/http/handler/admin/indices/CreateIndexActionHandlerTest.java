package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;
import com.github.obourgain.elasticsearch.http.response.admin.indices.createindex.CreateIndexResponse;

public class CreateIndexActionHandlerTest extends AbstractTest {

    @Before
    public void delete_existing_index() {
        transportClient.admin().indices().delete(Requests.deleteIndexRequest(THE_INDEX)).actionGet();
    }

    @Test
    public void should_create_index() throws ExecutionException, InterruptedException {
        CreateIndexResponse response = httpClient.admin().indices().createIndex(Requests.createIndexRequest(THE_INDEX)).get();
        Assertions.assertThat(response.isAcknowledged()).isTrue();
    }

    @Test
    public void should_create_index_with_settings() throws ExecutionException, InterruptedException {
        String settings = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/handler/admin/indices/create_index_with_settings.json");

        CreateIndexResponse response = httpClient.admin().indices()
                .createIndex(Requests.createIndexRequest(THE_INDEX)
                .settings(settings))
                .get();
        Assertions.assertThat(response.isAcknowledged()).isTrue();

        GetSettingsResponse getSettingsResponse = transportClient.admin().indices().getSettings(new GetSettingsRequest().indices(THE_INDEX)).actionGet();
        ImmutableOpenMap<String, Settings> indexToSettings = getSettingsResponse.getIndexToSettings();
        Assertions.assertThat(indexToSettings.iterator().hasNext()).isTrue();
        Assertions.assertThat(indexToSettings.iterator().next().key).isEqualTo(THE_INDEX);

        Settings expectedSettings = ImmutableSettings.builder().loadFromSource(settings).build();
        Settings actualSettings = indexToSettings.get(THE_INDEX);
        assertSettingsEquals(expectedSettings, actualSettings);
    }

    @Test
    public void should_create_index_with_mapping() throws ExecutionException, InterruptedException {
        String mapping = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/handler/admin/indices/create_index_with_mapping.json");

        CreateIndexResponse response = httpClient.admin().indices()
                .createIndex(Requests.createIndexRequest(THE_INDEX)
                .mapping(THE_TYPE, mapping))
                .get();
        Assertions.assertThat(response.isAcknowledged()).isTrue();

        refresh();

        GetMappingsResponse getMappingsResponse = transportClient.admin().indices().getMappings(new GetMappingsRequest().indices(THE_INDEX)).actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> indexToMappings = getMappingsResponse.getMappings();
        Assertions.assertThat(indexToMappings.iterator().hasNext()).isTrue();
        Assertions.assertThat(indexToMappings.iterator().next().key).isEqualTo(THE_INDEX);

        MappingMetaData actualMapping = indexToMappings.get(THE_INDEX).get(THE_TYPE);
        assertMappingsEquals(mappingAsJsonToMap(mapping), actualMapping);
    }

    // TODO test customs, warmers, aliases & creation date
}