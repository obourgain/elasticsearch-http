package com.github.obourgain.elasticsearch.http.handler.admin.indices.open;

import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.admin.indices.open.OpenIndexResponse;

public class OpenIndexActionHandlerTest extends AbstractTest {

    @Test
    public void should_open_index() throws Exception {
        CloseIndexResponse closeIndexResponse = transportClient.admin().indices().close(new CloseIndexRequest(THE_INDEX)).actionGet();
        Assertions.assertThat(closeIndexResponse.isAcknowledged()).isTrue();

        OpenIndexResponse openIndexResponse = httpClient.admin().indices().open(new OpenIndexRequest(THE_INDEX)).get();
        Assertions.assertThat(openIndexResponse.isAcknowledged()).isTrue();

        transportClient.admin().indices().recoveries(new RecoveryRequest(THE_INDEX)).actionGet();
    }
}