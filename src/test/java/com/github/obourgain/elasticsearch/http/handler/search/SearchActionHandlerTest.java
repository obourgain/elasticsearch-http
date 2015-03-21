package com.github.obourgain.elasticsearch.http.handler.search;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.lookup.SourceLookup;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.handler.search.search.SearchResponse;
import com.github.obourgain.elasticsearch.http.response.ElasticsearchHttpException;
import com.github.obourgain.elasticsearch.http.response.entity.aggs.Terms;
import com.google.common.collect.ImmutableMap;

public class SearchActionHandlerTest extends AbstractTest {

    @Test
    public void should_search() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);
        index(THE_INDEX, THE_TYPE, THE_ID, expected);

        refresh();

        SearchRequest searchRequest = new SearchRequest(THE_INDEX).types(THE_TYPE).source(new SearchSourceBuilder().query(matchAllQuery()));
        long start = System.currentTimeMillis();
        SearchResponse searchResponse = httpClient.search(searchRequest).get();
        long end = System.currentTimeMillis();

        Assertions.assertThat(searchResponse.getTookInMillis()).isLessThan(end - start);
        Assertions.assertThat(searchResponse.getScrollId()).isNull();

        assertShardsSuccessfulForIT(searchResponse.getShards(), THE_INDEX);
    }

    @Test
    public void should_search_with_template() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);
        index(THE_INDEX, THE_TYPE, THE_ID, expected);

        refresh();

        ImmutableMap<String, String> params = ImmutableMap.<String, String>builder()
                .put("the_string_field", "the_string_value")
                .put("the_integer_field", "42")
                .put("my_size", "5").build();
        SearchRequest searchRequest = new SearchRequest(THE_INDEX).types(THE_TYPE)
                .templateSource("{\n" +
                        "    \"query\": { \"match\" : { \"{{my_field}}\" : \"{{my_value}}\" } },\n" +
                        "    \"size\" : \"{{my_size}}\"\n" +
                        "}");
        searchRequest.templateParams(params);
        SearchResponse searchResponse = httpClient.search(searchRequest).get();

        Assertions.assertThat(searchResponse.getHits().getTotal()).isEqualTo(1);
    }

    @Test
    public void should_fail_on_invalid_query() throws IOException, ExecutionException, InterruptedException {
        SearchRequest searchRequest = new SearchRequest(THE_INDEX).types(THE_TYPE).source(new SearchSourceBuilder().query("invalid query"));
        try {
            httpClient.search(searchRequest).get();
            fail();
        } catch (ExecutionException e) {
            Assertions.assertThat(e).hasCauseInstanceOf(ElasticsearchHttpException.class);
            Assertions.assertThat(e.getMessage()).contains("status code 400");
            Assertions.assertThat(e.getMessage()).contains("Failed to parse source");
            Assertions.assertThat(e.getMessage()).contains("ElasticsearchParseException");
        }
    }

    @Test
    public void should_search_with_agg() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);
        index(THE_INDEX, THE_TYPE, THE_ID, expected);

        refresh();

        SearchRequest searchRequest = new SearchRequest(THE_INDEX).types(THE_TYPE)
                .source(new SearchSourceBuilder()
                        .query(matchAllQuery())
                        .aggregation(AggregationBuilders
                                .terms("foo")
                                .field("the_string_field")));

        SearchResponse searchResponse = httpClient.search(searchRequest).get();

        assertShardsSuccessfulForIT(searchResponse.getShards(), THE_INDEX);
        Assertions.assertThat(searchResponse.getAggregations()).isNotNull();
        Assertions.assertThat(searchResponse.getAggregations().getTerms("foo")).isNotNull();

        Assertions.assertThat(searchResponse.getAggregations().getTerms("foo").getDocCountErrorUpperBound()).isEqualTo(0);
        Assertions.assertThat(searchResponse.getAggregations().getTerms("foo").getSumOtherDocCount()).isEqualTo(0);
        Assertions.assertThat(searchResponse.getAggregations().getTerms("foo").getBuckets()).hasSize(1);
        Assertions.assertThat(searchResponse.getAggregations().getTerms("foo").getBuckets()).contains(new Terms.Bucket(0, "the_string_value", 1));
    }
}