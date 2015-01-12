package com.github.obourgain.elasticsearch.http.response.entity;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class TermVectorTest {

    @Test
    public void testParse() throws Exception {
        String json = readFromClasspath("json/termvector/termvector.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        TermVector termVector = TermVector.parse(parser);

        List<Term> terms = termVector.getTerms();
        Collections.sort(terms, new Comparator<Term>() {
            @Override
            public int compare(Term o1, Term o2) {
                return o1.getTerm().compareTo(o2.getTerm());
            }
        });

        assertThat(terms).hasSize(2);
        Term term = terms.get(0);
        TermTest.assertTestTerm(term);

        Term term2 = terms.get(1);
        TermTest.assertTwitterTerm(term2);

        assertTermVector(termVector);
    }

    public static void assertTermVector(TermVector termVector) {
        assertThat(termVector.getField()).isEqualTo("text");
        assertFieldStatistics(termVector);

        assertThat(termVector.getTerms()).hasSize(2);
        TermTest.assertTestTerm(termVector.getTerms().get(0));
        TermTest.assertTwitterTerm(termVector.getTerms().get(1));
    }

    public static void assertFieldStatistics(TermVector termVector) {
        assertThat(termVector.getFieldStatistics().getDocCount()).isEqualTo(2);
        assertThat(termVector.getFieldStatistics().getSumDocFreq()).isEqualTo(6);
        assertThat(termVector.getFieldStatistics().getSumTtf()).isEqualTo(8);
    }
}