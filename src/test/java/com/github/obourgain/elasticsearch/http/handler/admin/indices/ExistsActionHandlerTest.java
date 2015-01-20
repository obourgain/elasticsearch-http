package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.lookup.SourceLookup;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.document.exists.ExistsResponse;

public class ExistsActionHandlerTest extends AbstractTest {

    @Test
    public void should_return_true_when_not_exists() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);
        index(THE_INDEX, THE_TYPE, THE_ID, expected);

        refresh();

        ExistsRequest existsRequest = new ExistsRequest(THE_INDEX).types(THE_TYPE).source(new QuerySourceBuilder().setQuery(matchAllQuery()));
        ExistsResponse existsResponse = httpClient.exists(existsRequest).get();

        Assertions.assertThat(existsResponse.isExists()).isTrue();
    }

    @Test
    public void should_return_false_when_not_exists() throws IOException, ExecutionException, InterruptedException {
        ExistsRequest existsRequest = new ExistsRequest(THE_INDEX).types(THE_TYPE).source(new QuerySourceBuilder().setQuery(matchAllQuery()));
        ExistsResponse existsResponse = httpClient.exists(existsRequest).get();

        Assertions.assertThat(existsResponse.isExists()).isFalse();
    }

}