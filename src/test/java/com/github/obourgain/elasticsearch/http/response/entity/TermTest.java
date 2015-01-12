package com.github.obourgain.elasticsearch.http.response.entity;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class TermTest {

    @Test
    public void should_parse_term() throws Exception {
        String json = readFromClasspath("json/termvector/term.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        List<Term> terms = Term.parseTerms(parser);

        assertThat(terms).hasSize(1);
        Term term = terms.get(0);
        assertTestTerm(term);

        TokenTest.assertTestToken(term.getTokens());
    }

    @Test
    public void should_parse_several_terms() throws Exception {
        String json = readFromClasspath("json/termvector/term2.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        List<Term> terms = Term.parseTerms(parser);

        assertThat(terms).hasSize(2);
        Term term = terms.get(0);
        assertTestTerm(term);

        Term term2 = terms.get(1);
        assertTwitterTerm(term2);
    }

    public static void assertTestTerm(Term term) {
        assertThat(term.getTerm()).isEqualTo("test");
        assertThat(term.getDocFreq()).isEqualTo(2);
        assertThat(term.getTermFreq()).isEqualTo(3);
        assertThat(term.getTotalTermFreq()).isEqualTo(4);
        assertThat(term.getTokens()).hasSize(3);
        TokenTest.assertTestToken(term.getTokens());
    }

    public static void assertTwitterTerm(Term term) {
        assertThat(term.getTerm()).isEqualTo("twitter");
        assertThat(term.getDocFreq()).isEqualTo(2);
        assertThat(term.getTermFreq()).isEqualTo(1);
        assertThat(term.getTotalTermFreq()).isEqualTo(2);
        assertThat(term.getTokens()).hasSize(1);
        TokenTest.assertTwitterToken(term.getTokens());
    }
}