package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.admin.indices.optimize.OptimizeResponse;

public class OptimizeActionHandlerTest extends AbstractTest {

    @Test
    public void should_optimize() throws ExecutionException, InterruptedException {
        OptimizeResponse response = httpClient.admin().indices().optimize(new OptimizeRequest(THE_INDEX)).get();

        NumShards actualNumShards = getNumShards(THE_INDEX);
        Assertions.assertThat(response.getShards().getTotal()).isEqualTo(actualNumShards.totalNumShards);
    }

    @Test
    public void should_fail_when_index_does_not_exists() throws ExecutionException, InterruptedException {
        OptimizeResponse response = httpClient.admin().indices().optimize(new OptimizeRequest("foo")).get();
        Assertions.assertThat(response.getError()).contains("IndexMissingException");
    }

}