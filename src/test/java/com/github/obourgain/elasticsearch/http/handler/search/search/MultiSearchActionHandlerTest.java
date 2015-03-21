package com.github.obourgain.elasticsearch.http.handler.search.search;

import static org.elasticsearch.action.search.SearchType.COUNT;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;

public class MultiSearchActionHandlerTest extends AbstractTest {

    @Test
    public void should_do_multisearch() throws Exception {
        createSimpleDoc(THE_INDEX, THE_TYPE, "1");
        createSimpleDoc(THE_INDEX, THE_TYPE, "2");
        createSimpleDoc(THE_INDEX, THE_TYPE, "3");

        refresh();

        MultiSearchRequest request = new MultiSearchRequest()
                .add(new SearchRequest().indices(THE_INDEX).source(new SearchSourceBuilder().query(matchAllQuery())).searchType(COUNT))
                .add(new SearchRequest().indices(THE_INDEX).source(new SearchSourceBuilder().query(matchQuery("the_string_field", "the_string_value"))))
                .add(new SearchRequest().indices(THE_INDEX).source(new SearchSourceBuilder().query(matchQuery("the_string_field", "the_string_value")).from(1).size(1)))
                ;

        MultiSearchResponse response = httpClient.multiSearch(request).get();

        List<SearchResponse> responses = response.getResponses();
        Assertions.assertThat(responses).hasSize(3);

        Assertions.assertThat(responses.get(0).getHits().getTotal()).isEqualTo(3);
        Assertions.assertThat(responses.get(0).getHits().getHits()).isEmpty();

        Assertions.assertThat(responses.get(1).getHits().getTotal()).isEqualTo(3);
        Assertions.assertThat(responses.get(1).getHits().getHits()).hasSize(3);

        Assertions.assertThat(responses.get(2).getHits().getTotal()).isEqualTo(3);
        Assertions.assertThat(responses.get(2).getHits().getHits()).hasSize(1);
        Assertions.assertThat(responses.get(2).getHits().getHits().get(0).getSource().length).isGreaterThan(1);
    }
}