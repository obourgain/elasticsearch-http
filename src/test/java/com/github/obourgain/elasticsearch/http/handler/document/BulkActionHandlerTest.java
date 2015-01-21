package com.github.obourgain.elasticsearch.http.handler.document;

import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;

public class BulkActionHandlerTest extends AbstractTest {

    @Test
    public void should_execute_bulk() throws ExecutionException, InterruptedException {
        BulkResponse response = httpClient.bulk(new BulkRequest()).get();

        Assertions.assertThat(response.hasFailures()).isFalse();
    }

}