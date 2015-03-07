package com.github.obourgain.elasticsearch.http.handler.admin.indices.settings;

import static java.util.Collections.singletonMap;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.admin.indices.settings.UpdateSettingsResponse;

public class UpdateSettingsActionHandlerTest extends AbstractTest {

    @Test
    public void should_update_settings() throws Exception {
        String index = "to_update_indices";
        createIndex(index);
        GetSettingsResponse initial = transportClient.admin().indices().getSettings(new GetSettingsRequest().indices(index)).actionGet();
        Assertions.assertThat(initial.getIndexToSettings().get(index).get("index.number_of_replicas")).isNotEqualTo("5");

        UpdateSettingsResponse response = httpClient.admin().indices().updateSettings(
                new UpdateSettingsRequest(index)
                        .settings(singletonMap("index.number_of_replicas", 5))
        ).get();

        Assertions.assertThat(response.isAcknowledged()).isTrue();

        GetSettingsResponse expected = transportClient.admin().indices().getSettings(new GetSettingsRequest().indices(index)).actionGet();
        Assertions.assertThat(expected.getIndexToSettings().get(index).get("index.number_of_replicas")).isEqualTo("5");
    }

}