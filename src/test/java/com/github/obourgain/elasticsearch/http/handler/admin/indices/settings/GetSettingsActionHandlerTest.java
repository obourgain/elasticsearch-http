package com.github.obourgain.elasticsearch.http.handler.admin.indices.settings;

import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.admin.indices.settings.GetSettingsResponse;

public class GetSettingsActionHandlerTest extends AbstractTest {

    @Test
    public void should_get_settings() throws Exception {
        GetSettingsResponse response = httpClient.admin().indices().getSettings(new GetSettingsRequest().indices(THE_INDEX)).get();

        Assertions.assertThat(response.getIndexToSettings()).hasSize(1);

        org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse transportResponse = transportClient.admin().indices().getSettings(new GetSettingsRequest().indices(THE_INDEX)).actionGet();
        Settings expected = transportResponse.getIndexToSettings().get(THE_INDEX);

        Assertions.assertThat(response.getIndexToSettings().get(THE_INDEX)).isEqualTo(expected);
    }

    // TODO more tests
}