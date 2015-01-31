package com.github.obourgain.elasticsearch.http.handler.search;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.lookup.SourceLookup;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.ElasticsearchHttpException;
import com.github.obourgain.elasticsearch.http.handler.search.count.CountResponse;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;

public class CountActionHandlerTest extends AbstractTest {

    @Test
    public void should_count() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);
        index(THE_INDEX, THE_TYPE, THE_ID, expected);

        refresh();

        CountRequest countRequest = new CountRequest(THE_INDEX).types(THE_TYPE).source(new QuerySourceBuilder().setQuery(matchAllQuery()));
        CountResponse countResponse = httpClient.count(countRequest).get();

        Shards shards = countResponse.getShards();
        Assertions.assertThat(shards.getTotal()).isEqualTo(getNumShards(THE_INDEX).numPrimaries);
        Assertions.assertThat(shards.getSuccessful()).isEqualTo(getNumShards(THE_INDEX).numPrimaries);
        Assertions.assertThat(shards.getFailed()).isEqualTo(0);
    }

    @Test
    public void should_fail_on_invalid_query() throws IOException, ExecutionException, InterruptedException {
        CountRequest countRequest = new CountRequest(THE_INDEX).types(THE_TYPE).source("{invalid query}");
        try {
            httpClient.count(countRequest).get();
            fail();
        } catch (ExecutionException e) {
            Assertions.assertThat(e).hasCauseInstanceOf(ElasticsearchHttpException.class);
            Assertions.assertThat(e.getMessage()).contains("status code 400");
            Assertions.assertThat(e.getMessage()).contains("Failed to parse");
            Assertions.assertThat(e.getMessage()).contains("QueryParsingException");
        }
    }
}