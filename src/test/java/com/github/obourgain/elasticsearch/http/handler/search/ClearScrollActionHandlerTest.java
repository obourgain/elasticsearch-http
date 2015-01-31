package com.github.obourgain.elasticsearch.http.handler.search;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.handler.search.clearscroll.ClearScrollResponse;
import com.github.obourgain.elasticsearch.http.handler.search.search.SearchResponse;

public class ClearScrollActionHandlerTest extends AbstractTest {

    @Test
    public void should_clear_scroll() throws IOException, ExecutionException, InterruptedException {
        SearchRequest searchRequest = new SearchRequest(THE_INDEX).types(THE_TYPE).source(new SearchSourceBuilder().query(matchAllQuery())).scroll("1m");
        SearchResponse searchResponse = httpClient.search(searchRequest).get();

        String scrollId = searchResponse.getScrollId();
        Assertions.assertThat(scrollId).isNotEmpty();

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse response = httpClient.clearScroll(clearScrollRequest).get();

        Assertions.assertThat(response.isSucceeded()).isTrue();
    }

    @Test
    public void should_return_failed_if_scroll_does_not_exists() throws IOException, ExecutionException, InterruptedException {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId("cXVlcnlUaGVuRmV0Y2g7NTs2OmZXdGUtMmVkVEpteHBDa2RFSVNoZGc7ODpmV3RlLTJlZFRKbXhwQ2tkRUlTaGRnOzk6Zld0ZS0yZWRUSm14cENrZEVJU2hkZzs3OmZXdGUtMmVkVEpteHBDa2RFSVNoZGc7MTA6Zld0ZS0yZWRUSm14cENrZEVJU2hkZzswOw");
        ClearScrollResponse response = httpClient.clearScroll(clearScrollRequest).get();

        Assertions.assertThat(response.isSucceeded()).isFalse();
    }

}