package com.github.obourgain.elasticsearch.http.handler.search;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.lookup.SourceLookup;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.ElasticsearchHttpException;
import com.github.obourgain.elasticsearch.http.response.entity.Explanation;
import com.github.obourgain.elasticsearch.http.response.search.explain.ExplainResponse;

public class ExplainActionHandlerTest extends AbstractTest {

    @Test
    public void should_search() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);
        index(THE_INDEX, THE_TYPE, THE_ID, expected);

        refresh();

        ExplainRequest request = new ExplainRequest(THE_INDEX, THE_TYPE, THE_ID);
        request.source(new QuerySourceBuilder().setQuery(QueryBuilders.matchAllQuery()));
        ExplainResponse response = httpClient.explain(request).get();

        Assertions.assertThat(response.getIndex()).isEqualTo(THE_INDEX);
        Assertions.assertThat(response.getType()).isEqualTo(THE_TYPE);
        Assertions.assertThat(response.getId()).isEqualTo(THE_ID);

        Assertions.assertThat(response.isMatched()).isTrue();

        Explanation explanation = response.getExplanation();
        Assertions.assertThat(explanation.getValue()).isEqualTo(1);
        Assertions.assertThat(explanation.getDescription()).contains("ConstantScore");
    }

    @Test
    public void should_not_match_on_invalid_query() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);
        index(THE_INDEX, THE_TYPE, THE_ID, expected);

        refresh();

        ExplainRequest request = new ExplainRequest(THE_INDEX, THE_TYPE, THE_ID);
        request.source(new BytesArray("{\"invalid_query\":\"bar\"}"), true);

        try {
            httpClient.explain(request).get();
        }catch (ExecutionException e) {
            Assertions.assertThat(e.getCause()).isInstanceOf(ElasticsearchHttpException.class);
            Assertions.assertThat(e.getCause().getMessage()).contains("request does not support");
        }
    }
}