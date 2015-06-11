package com.github.obourgain.elasticsearch.http.handler.search;

import java.io.IOException;
import java.lang.reflect.Field;
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
import com.github.obourgain.elasticsearch.http.handler.search.explain.ExplainResponse;
import com.github.obourgain.elasticsearch.http.response.ElasticsearchHttpException;
import com.github.obourgain.elasticsearch.http.response.entity.Explanation;

public class ExplainActionHandlerTest extends AbstractTest {

    @Test
    public void should_search() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);
        index(THE_INDEX, THE_TYPE, THE_ID, expected);

        ensureSearchable(THE_INDEX);
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
        setSource(request, new BytesArray("{\"invalid_query\":\"bar\"}"));

        try {
            httpClient.explain(request).get();
        }catch (ExecutionException e) {
            Assertions.assertThat(e.getCause()).isInstanceOf(ElasticsearchHttpException.class);
            Assertions.assertThat(e.getCause().getMessage()).contains("request does not support");
        }
    }

    private void setSource(ExplainRequest request, BytesArray source) {
        // workaround as the method to set the source has changed between 1.5.x and 1.6 (the boolean param was removed)
        try {
            Field field = ExplainRequest.class.getDeclaredField("source");
            field.setAccessible(true);
            field.set(request, source);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}