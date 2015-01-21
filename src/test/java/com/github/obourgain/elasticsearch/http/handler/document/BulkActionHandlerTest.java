package com.github.obourgain.elasticsearch.http.handler.document;

import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;

public class BulkActionHandlerTest extends AbstractTest {

    @Test
    public void should_execute_bulk() throws ExecutionException, InterruptedException {
        BulkResponse response = httpClient.bulk(new BulkRequest()).get();

        Assertions.assertThat(response.hasFailures()).isFalse();
    }

    @Test
    public void should_execute_bulk2() throws ExecutionException, InterruptedException {
        BulkRequest request = new BulkRequest();

        IndexRequest action = new IndexRequest();
        action.source("foo", "bar");
        action.index("the_index");
        action.type("the_type");
        action.id("the_id");

        request.add(action);
        BulkResponse response = httpClient.bulk(request).get();

        Assertions.assertThat(response.hasFailures()).isFalse();
    }

}