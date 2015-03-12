package com.github.obourgain.elasticsearch.http.handler.search.suggest;

import static org.elasticsearch.search.suggest.SuggestBuilders.completionSuggestion;
import static org.elasticsearch.search.suggest.SuggestBuilders.termSuggestion;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.suggest.SuggestRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.jackson.dataformat.yaml.snakeyaml.emitter.EmitterException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.entity.suggest.Completion;

public class SuggestActionHandlerTest extends AbstractTest {

    static XContentType savedContentType;

    @Test
    @Ignore("suggestion are not serialized correctly as of 1.4.4, missing the start_object")
    public void should_suggest() throws Exception {
        createIndex("music");

        createDocFromClasspathFile("music", "song", "1", "com/github/obourgain/elasticsearch/http/handler/search/suggest/doc1.json");
        createDocFromClasspathFile("music", "song", "2", "com/github/obourgain/elasticsearch/http/handler/search/suggest/doc2.json");
        createDocFromClasspathFile("music", "song", "3", "com/github/obourgain/elasticsearch/http/handler/search/suggest/doc3.json");

        TermSuggestionBuilder termSuggestionBuilder = termSuggestion("terms-suggest").text("everythign").field("name");
        SuggestRequest suggest = new SuggestRequest("music")
                .suggest(termSuggestionBuilder);
        SuggestResponse response = httpClient.suggest(suggest).get();

        Assertions.assertThat(response.getSuggestions()).isNotNull();
        Assertions.assertThat(response.getSuggestions().names()).hasSize(1);

        Completion completion = response.getSuggestions().getCompletion("song-suggest");
        Assertions.assertThat(completion.getOptions()).hasSize(3);

//        Term term = response.getSuggestions().getTerm("term-suggest");
//        Assertions.assertThat(term.getOptions()).hasSize(1);
    }

    @Test(expected = EmitterException.class)
    public void should_fail_on_yaml() throws Exception {
        // org.elasticsearch.common.jackson.dataformat.yaml.snakeyaml.emitter.EmitterException: expected DocumentEndEvent, but got <org.elasticsearch.common.jackson.dataformat.yaml.snakeyaml.events.MappingStartEvent(anchor=null, tag=null, implicit=true)>
        XContentFactory.yamlBuilder()
                .startObject()
                .startObject()
                .endObject()
                .endObject()
                ;

    }
}
