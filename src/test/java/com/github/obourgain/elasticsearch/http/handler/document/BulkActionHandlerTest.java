package com.github.obourgain.elasticsearch.http.handler.document;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.document.bulk.BulkResponse;

public class BulkActionHandlerTest extends AbstractTest {

    @Test
    public void should_fail_for_empty() throws ExecutionException, InterruptedException {
        try {
            httpClient.bulk(new BulkRequest()).get();
            fail();
        } catch (Exception e) {
            Assertions.assertThat(e).hasMessageContaining("ElasticsearchParseException[Failed to derive xcontent");
            Assertions.assertThat(e).hasMessageContaining("status code 400");
        }
    }

    @Test
    public void should_execute_bulk() throws ExecutionException, InterruptedException, IOException {
        BulkRequest request = new BulkRequest();

        IndexRequest action = new IndexRequest();
        action.source(source());
        action.index("the_index");
        action.type("the_type");
        action.id("the_id");

        request.add(action);
        request.add(action);
        request.add(action);
        BulkResponse response = httpClient.bulk(request).get();

        Assertions.assertThat(response.isErrors()).isFalse();
    }
}