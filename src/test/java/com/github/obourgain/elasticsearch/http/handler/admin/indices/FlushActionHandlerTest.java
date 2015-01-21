package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.admin.indices.flush.FlushResponse;

public class FlushActionHandlerTest extends AbstractTest {

    @Test
    public void should_flush() throws Exception {
        FlushResponse response = httpClient.admin().indices().flush(new FlushRequest(THE_INDEX)).get();

        NumShards actualNumShards = getNumShards(THE_INDEX);
        Assertions.assertThat(response.getShards().getTotal()).isEqualTo(actualNumShards.totalNumShards);
    }

    @Test
    public void should_fail_when_index_does_not_exists() throws Exception {
        FlushResponse response = httpClient.admin().indices().flush(new FlushRequest("foo")).get();
        Assertions.assertThat(response.getError()).contains("IndexMissingException");
    }

}