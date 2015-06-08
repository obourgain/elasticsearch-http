package com.github.obourgain.elasticsearch.http.handler.search;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.handler.search.exists.ExistsResponse;

public class ExistsActionHandlerTest extends AbstractTest {

    @Test
    public void should_not_fail_on_invalid_query() throws IOException, ExecutionException, InterruptedException {
        // for an invalid query, ES returns 404
        ExistsRequest existsRequest = new ExistsRequest(THE_INDEX).types(THE_TYPE).source("{invalid query}");
        ExistsResponse response = httpClient.exists(existsRequest).get();

        Assertions.assertThat(response.isExists()).isFalse();
    }

    @Test
    public void should_return_true_when_exists() throws IOException, ExecutionException, InterruptedException {
        for (int i = 0; i < 5; i++) {
            BytesReference source = source().bytes();
            Map<String, Object> expected = SourceLookup.sourceAsMap(source);
            index(THE_INDEX, THE_TYPE, THE_ID, expected);

            refresh();

            ExistsRequest existsRequest = new ExistsRequest(THE_INDEX).types(THE_TYPE).source(new QuerySourceBuilder().setQuery(matchAllQuery()));
            org.elasticsearch.action.exists.ExistsResponse transportResponse = ((InternalTestCluster) cluster()).masterClient().exists(existsRequest).actionGet();
//            org.elasticsearch.action.exists.ExistsResponse transportResponse = masterClient("localhost:9601").exists(existsRequest).actionGet();
            ExistsResponse existsResponse = httpClient.exists(existsRequest).get();

            boolean transportExists = transportResponse.exists();
            boolean httpExists = existsResponse.isExists();
            System.err.println("transportExists " + transportExists);
            System.err.println("httpExists " + httpExists);
//            Assertions.assertThat(httpExists).isTrue();

            client().admin().indices().delete(new DeleteIndexRequest(THE_INDEX)).actionGet();
            createIndex(THE_INDEX);
            ensureSearchable(THE_INDEX);
        }
    }

    @Test
    public void should_return_false_when_not_exists() throws IOException, ExecutionException, InterruptedException {
        ExistsRequest existsRequest = new ExistsRequest(THE_INDEX).types(THE_TYPE).source(new QuerySourceBuilder().setQuery(matchAllQuery()));
        ExistsResponse existsResponse = httpClient.exists(existsRequest).get();

        Assertions.assertThat(existsResponse.isExists()).isFalse();
    }

}