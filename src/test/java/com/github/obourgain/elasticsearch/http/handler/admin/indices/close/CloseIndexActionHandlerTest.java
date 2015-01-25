package com.github.obourgain.elasticsearch.http.handler.admin.indices.close;

import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.indices.IndexClosedException;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.admin.indices.close.CloseIndexResponse;

public class CloseIndexActionHandlerTest extends AbstractTest {

    @Test
    public void should_close_index() throws Exception {
        CloseIndexResponse closeIndexResponse = httpClient.admin().indices().close(new CloseIndexRequest(THE_INDEX)).get();
        Assertions.assertThat(closeIndexResponse.isAcknowledged()).isTrue();

        try {
            transportClient.admin().indices().recoveries(new RecoveryRequest(THE_INDEX)).actionGet();
            fail();
        } catch (IndexClosedException ignored) {

        }
    }
}