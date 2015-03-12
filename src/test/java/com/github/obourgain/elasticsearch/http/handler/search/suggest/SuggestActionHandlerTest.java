package com.github.obourgain.elasticsearch.http.handler.search.suggest;

import static org.elasticsearch.search.suggest.SuggestBuilders.completionSuggestion;
import static org.elasticsearch.search.suggest.SuggestBuilders.termSuggestion;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.entity.suggest.Completion;
import com.github.obourgain.elasticsearch.http.response.entity.suggest.Term;

public class SuggestActionHandlerTest extends AbstractTest {

    @Test
    public void should_suggest() throws Exception {
        createIndex("music");

        createDocFromClasspathFile("music", "song", "1", "com/github/obourgain/elasticsearch/http/handler/search/suggest/doc1.json");
        createDocFromClasspathFile("music", "song", "2", "com/github/obourgain/elasticsearch/http/handler/search/suggest/doc2.json");
        createDocFromClasspathFile("music", "song", "3", "com/github/obourgain/elasticsearch/http/handler/search/suggest/doc3.json");

        TermSuggestionBuilder termSuggestionBuilder = termSuggestion("terms-suggest").text("everythign").field("name");
        SuggestRequest suggest = new SuggestRequest("music")
//                        .suggest(completionSuggestion("song-suggest").text("n").field("name"))
                .suggest(termSuggestionBuilder);
        SuggestResponse response = httpClient.suggest(suggest
        ).get();

        Assertions.assertThat(response.getSuggestions()).isNotNull();
        Assertions.assertThat(response.getSuggestions().names()).hasSize(1);

        Completion completion = response.getSuggestions().getCompletion("song-suggest");
        Assertions.assertThat(completion.getOptions()).hasSize(3);

//        Term term = response.getSuggestions().getTerm("term-suggest");
//        Assertions.assertThat(term.getOptions()).hasSize(1);
    }
}
