package com.github.obourgain.elasticsearch.http.handler.search.search;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.bytes.BytesArray;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;
import com.github.obourgain.elasticsearch.http.response.entity.Hit;
import com.github.obourgain.elasticsearch.http.response.entity.aggs.Terms;
import com.github.obourgain.elasticsearch.http.response.entity.suggest.Completion;
import com.github.obourgain.elasticsearch.http.response.entity.suggest.Suggestions;
import com.github.obourgain.elasticsearch.http.response.entity.suggest.Term;

public class SearchResponseTest {

    @Test
    public void should_parse_response() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/handler/search/search/response.json");

        SearchResponse searchResponse = new SearchResponse().parse(new BytesArray(json));

        assertThat(searchResponse.getShards().getTotal()).isEqualTo(20);
        assertThat(searchResponse.getShards().getSuccessful()).isEqualTo(20);
        assertThat(searchResponse.getShards().getFailed()).isEqualTo(0);

        assertThat(searchResponse.getScrollId()).isNull();
        assertThat(searchResponse.getTookInMillis()).isEqualTo(11);
        assertThat(searchResponse.isTimedOut()).isFalse();

        assertThat(searchResponse.getHits().getTotal()).isEqualTo(1);
        assertThat(searchResponse.getHits().getMaxScore()).isEqualTo(1);

        List<Hit> hits = searchResponse.getHits().getHits();
        assertThat(hits).hasSize(1);
        assertThat(hits.get(0).getIndex()).isEqualTo("the_index");
        assertThat(hits.get(0).getType()).isEqualTo("the_type");
        assertThat(hits.get(0).getId()).isEqualTo("the_id");
        assertThat(hits.get(0).getScore()).isEqualTo(1);
        assertThat(hits.get(0).getSource().length).isGreaterThan(1);
    }

    @Test
    public void should_parse_response_with_aggs() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/handler/search/search/response_with_agg.json");

        SearchResponse searchResponse = new SearchResponse().parse(new BytesArray(json));

        assertThat(searchResponse.getShards().getTotal()).isEqualTo(20);
        assertThat(searchResponse.getShards().getSuccessful()).isEqualTo(20);
        assertThat(searchResponse.getShards().getFailed()).isEqualTo(0);

        assertThat(searchResponse.getScrollId()).isNull();
        assertThat(searchResponse.getTookInMillis()).isEqualTo(11);
        assertThat(searchResponse.isTimedOut()).isFalse();

        assertThat(searchResponse.getHits().getTotal()).isEqualTo(1);
        assertThat(searchResponse.getHits().getMaxScore()).isEqualTo(1);

        List<Hit> hits = searchResponse.getHits().getHits();
        assertThat(hits).hasSize(1);
        assertThat(hits.get(0).getIndex()).isEqualTo("the_index");
        assertThat(hits.get(0).getType()).isEqualTo("the_type");
        assertThat(hits.get(0).getId()).isEqualTo("the_id");
        assertThat(hits.get(0).getScore()).isEqualTo(1);
        assertThat(hits.get(0).getSource().length).isGreaterThan(1);

        assertThat(searchResponse.getAggregations()).isNotNull();
        Terms words = searchResponse.getAggregations().getTerms("words");
        assertThat(words).isNotNull();
        assertThat(words.getBuckets()).hasSize(2);
        assertThat(words.getDocCountErrorUpperBound()).isEqualTo(3);
        assertThat(words.getSumOtherDocCount()).isEqualTo(2);
    }

    @Test
    public void should_parse_response_with_fields() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/handler/search/search/response_with_fields.json");

        SearchResponse searchResponse = new SearchResponse().parse(new BytesArray(json));

        assertThat(searchResponse.getHits().getTotal()).isEqualTo(2);
        assertThat(searchResponse.getHits().getMaxScore()).isEqualTo(1);

        List<Hit> hits = searchResponse.getHits().getHits();
        assertThat(hits).hasSize(2);
        assertThat(hits.get(0).getIndex()).isEqualTo("twitter2");
        assertThat(hits.get(0).getType()).isEqualTo("tweet");
        assertThat(hits.get(0).getId()).isEqualTo("1");
        assertThat(hits.get(0).getScore()).isEqualTo(1);
        assertThat(hits.get(0).getSource()).isNull();

        assertThat(hits.get(0).getFields()).hasSize(2);
    }

    @Test
    public void should_parse_response_with_highlights() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/handler/search/search/response_with_highlight.json");

        SearchResponse searchResponse = new SearchResponse().parse(new BytesArray(json));

        assertThat(searchResponse.getHits().getTotal()).isEqualTo(2);

        List<Hit> hits = searchResponse.getHits().getHits();
        assertThat(hits).hasSize(2);
        assertThat(hits.get(0).getIndex()).isEqualTo("twitter2");
        assertThat(hits.get(0).getType()).isEqualTo("tweet");
        assertThat(hits.get(0).getId()).isEqualTo("2");
        assertThat(hits.get(0).getSource().length).isGreaterThan(1);
        assertThat(hits.get(0).getHighlights()).hasSize(1).containsKey("message");
        assertThat(hits.get(0).getHighlights().get("message").getValue()).isEqualTo("the <em>message</em>");

        assertThat(hits.get(1).getIndex()).isEqualTo("twitter2");
        assertThat(hits.get(1).getType()).isEqualTo("tweet");
        assertThat(hits.get(1).getId()).isEqualTo("1");
        assertThat(hits.get(1).getSource().length).isGreaterThan(1);
        assertThat(hits.get(1).getHighlights()).hasSize(1).containsKey("message");
        assertThat(hits.get(1).getHighlights().get("message").getValue()).isEqualTo("the <em>message</em>");
    }

    @Test
    public void should_parse_response_with_failures() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/handler/search/search/response_with_failures.json");

        SearchResponse searchResponse = new SearchResponse().parse(new BytesArray(json));

        Assertions.assertThat(searchResponse.getShards().getTotal()).isEqualTo(5);
        Assertions.assertThat(searchResponse.getShards().getSuccessful()).isEqualTo(2);
        Assertions.assertThat(searchResponse.getShards().getFailed()).isEqualTo(3);
        Assertions.assertThat(searchResponse.getShards().getFailures()).hasSize(3);

        assertThat(searchResponse.getHits().getTotal()).isEqualTo(0);
        assertThat(searchResponse.getHits().getHits()).isEmpty();
    }

    @Test
    public void should_parse_response_with_suggestions() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/handler/search/search/response_with_suggestions.json");

        SearchResponse searchResponse = new SearchResponse().parse(new BytesArray(json));

        assertThat(searchResponse.getHits().getTotal()).isEqualTo(3);
        assertThat(searchResponse.getHits().getHits()).isNotEmpty();

        Suggestions suggestions = searchResponse.getSuggestions();
        Assertions.assertThat(suggestions).isNotNull();

        Term term = suggestions.getTerm("terms-suggest");
        Assertions.assertThat(term.getText()).isEqualTo("everythign");
        Assertions.assertThat(term.getOffset()).isEqualTo(0);
        Assertions.assertThat(term.getLength()).isEqualTo(10);
        Assertions.assertThat(term.getOptions()).hasSize(1);

        Assertions.assertThat(term.getOptions().get(0).getFreq()).isEqualTo(1);
        Assertions.assertThat(term.getOptions().get(0).getText()).isEqualTo("everything");
        Assertions.assertThat(term.getOptions().get(0).getScore()).isEqualTo(0.9f);

        Completion completion = suggestions.getCompletion("song-suggest");

        Assertions.assertThat(completion.getText()).isEqualTo("n");
        Assertions.assertThat(completion.getOffset()).isEqualTo(0);
        Assertions.assertThat(completion.getLength()).isEqualTo(1);
        Assertions.assertThat(completion.getOptions()).hasSize(3);

        Assertions.assertThat(completion.getOptions().get(0).getPayload()).isNull();
        Assertions.assertThat(completion.getOptions().get(0).getText()).isEqualTo("Metallica - Nothing else Matters");
        Assertions.assertThat(completion.getOptions().get(0).getScore()).isEqualTo(56);

        Assertions.assertThat(completion.getOptions().get(1).getPayload()).isNull();
        Assertions.assertThat(completion.getOptions().get(1).getText()).isEqualTo("Death - Nothing is Everything");
        Assertions.assertThat(completion.getOptions().get(1).getScore()).isEqualTo(42);

        Assertions.assertThat(completion.getOptions().get(2).getPayload()).isEqualTo("{\"artistId\":2321}");
        Assertions.assertThat(completion.getOptions().get(2).getText()).isEqualTo("Nirvana - Nevermind");
        Assertions.assertThat(completion.getOptions().get(2).getScore()).isEqualTo(34);
    }
}