package com.github.obourgain.elasticsearch.http.handler.document.termvector;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.entity.FieldStatistics;
import com.github.obourgain.elasticsearch.http.response.entity.Term;
import com.github.obourgain.elasticsearch.http.response.entity.TermVector;
import com.github.obourgain.elasticsearch.http.response.entity.TermVectorTest;
import com.github.obourgain.elasticsearch.http.response.entity.Token;
import com.github.obourgain.elasticsearch.http.response.termvectors.TermVectorResponse;
import com.github.obourgain.elasticsearch.http.response.termvectors.TermVectorResponseParserTest;

public class TermVectorsActionHandlerTest extends AbstractTest {

    public static final String TWITTER = "twitter";
    public static final String TWEET = "tweet";

    @Test
    public void should_get_term_vector() throws IOException, ExecutionException, InterruptedException {
        createIndexAndDocs();

        TermVectorRequest request = new TermVectorRequest().index(TWITTER).type(TWEET).id("1")
                .offsets(true)
                .payloads(true)
                .positions(true)
                .termStatistics(true)
                .fieldStatistics(true)
                .selectedFields(new String[]{"text"});

        TermVectorResponse response = httpClient.termVectors(request).get();
        TermVectorResponseParserTest.assertTermVectorResponse(response);
    }

    @Test
    public void should_fetch_offsets() throws IOException, ExecutionException, InterruptedException {
        createIndexAndDocs();

        TermVectorRequest request = new TermVectorRequest().index(TWITTER).type(TWEET).id("1")
                .offsets(true)
                .payloads(false)
                .positions(false)
                .termStatistics(false)
                .fieldStatistics(false)
                .selectedFields(new String[]{"text"});

        TermVectorResponse response = httpClient.termVectors(request).get();
        Assertions.assertThat(response.getTermVector().getFieldStatistics()).isNull();

        List<Term> terms = response.getTermVector().getTerms();
        for (Term term : terms) {
            List<Token> tokens = term.getTokens();
            for (Token token : tokens) {
                Assertions.assertThat(token.getStartOffset()).isNotNull();
                Assertions.assertThat(token.getEndOffset()).isNotNull();
                Assertions.assertThat(token.getPayload()).isNull();
                Assertions.assertThat(token.getPosition()).isNull();
            }
        }
    }

    @Test
    public void should_fetch_payloads() throws IOException, ExecutionException, InterruptedException {
        createIndexAndDocs();

        TermVectorRequest request = new TermVectorRequest().index(TWITTER).type(TWEET).id("1")
                .offsets(false)
                .payloads(true)
                .positions(false)
                .termStatistics(false)
                .fieldStatistics(false)
                .selectedFields(new String[]{"text"});

        TermVectorResponse response = httpClient.termVectors(request).get();
        Assertions.assertThat(response.getTermVector().getFieldStatistics()).isNull();

        List<Term> terms = response.getTermVector().getTerms();
        for (Term term : terms) {
            List<Token> tokens = term.getTokens();
            for (Token token : tokens) {
                Assertions.assertThat(token.getStartOffset()).isNull();
                Assertions.assertThat(token.getEndOffset()).isNull();
                Assertions.assertThat(token.getPayload()).isNotNull();
                Assertions.assertThat(token.getPosition()).isNull();
            }
        }
    }

    @Test
    public void should_fetch_position() throws IOException, ExecutionException, InterruptedException {
        createIndexAndDocs();

        TermVectorRequest request = new TermVectorRequest().index(TWITTER).type(TWEET).id("1")
                .offsets(false)
                .payloads(false)
                .positions(true)
                .termStatistics(false)
                .fieldStatistics(false)
                .selectedFields(new String[]{"text"});

        TermVectorResponse response = httpClient.termVectors(request).get();
        Assertions.assertThat(response.getTermVector().getFieldStatistics()).isNull();

        List<Term> terms = response.getTermVector().getTerms();
        for (Term term : terms) {
            List<Token> tokens = term.getTokens();
            for (Token token : tokens) {
                Assertions.assertThat(token.getStartOffset()).isNull();
                Assertions.assertThat(token.getEndOffset()).isNull();
                Assertions.assertThat(token.getPayload()).isNull();
                Assertions.assertThat(token.getPosition()).isNotNull();
            }
        }
    }

    @Test
    public void should_fetch_term_statistics() throws IOException, ExecutionException, InterruptedException {
        createIndexAndDocs();

        TermVectorRequest request = new TermVectorRequest().index(TWITTER).type(TWEET).id("1")
                .offsets(false)
                .payloads(false)
                .positions(false)
                .termStatistics(true)
                .fieldStatistics(false)
                .selectedFields(new String[]{"text"});

        TermVectorResponse response = httpClient.termVectors(request).get();

        TermVector termVector = response.getTermVector();
        Assertions.assertThat(termVector.getFieldStatistics()).isNull();

        List<Term> terms = termVector.getTerms();
        for (Term term : terms) {
            Assertions.assertThat(term.getTokens()).isNull();
            Assertions.assertThat(term.getDocFreq()).isNotNull();
            Assertions.assertThat(term.getTermFreq()).isNotNull();
            Assertions.assertThat(term.getTotalTermFreq()).isNotNull();
        }
    }

    @Test
    public void should_fetch_field_statistics() throws IOException, ExecutionException, InterruptedException {
        createIndexAndDocs();

        TermVectorRequest request = new TermVectorRequest().index(TWITTER).type(TWEET).id("1")
                .offsets(false)
                .payloads(false)
                .positions(false)
                .termStatistics(false)
                .fieldStatistics(true)
                .selectedFields(new String[]{"text"});

        TermVectorResponse response = httpClient.termVectors(request).get();

        FieldStatistics fieldStatistics = response.getTermVector().getFieldStatistics();
        Assertions.assertThat(fieldStatistics).isNotNull();
        Assertions.assertThat(fieldStatistics.getDocCount()).isEqualTo(2);
        Assertions.assertThat(fieldStatistics.getSumDocFreq()).isEqualTo(6);
        Assertions.assertThat(fieldStatistics.getSumTtf()).isEqualTo(8);

        List<Term> terms = response.getTermVector().getTerms();
        for (Term term : terms) {
            Assertions.assertThat(term.getTokens()).isNull();
        }
    }

    private void createIndexAndDocs() {
        String settingsSource = readFromClasspath("com/github/obourgain/elasticsearch/http/handler/document/termvector/settings.json");
        ImmutableSettings.Builder settings = ImmutableSettings.builder().loadFromSource(settingsSource);
        prepareCreate(TWITTER, 0, settings).execute().actionGet();

        String mappingSource = readFromClasspath("com/github/obourgain/elasticsearch/http/handler/document/termvector/mapping.json");
        transportClient.admin().indices().putMapping(Requests.putMappingRequest(TWITTER).type(TWEET).source(mappingSource)).actionGet();

        String doc1 = readFromClasspath("com/github/obourgain/elasticsearch/http/handler/document/termvector/doc1.json");
        String doc2 = readFromClasspath("com/github/obourgain/elasticsearch/http/handler/document/termvector/doc2.json");
        transportClient.index(Requests.indexRequest().index(TWITTER).type(TWEET).id("1").source(doc1)).actionGet();
        transportClient.index(Requests.indexRequest().index(TWITTER).type(TWEET).id("2").source(doc2)).actionGet();
        ensureSearchable(TWITTER);
        refresh();
    }
}