package com.github.obourgain.elasticsearch.http.handler.document.termvectors;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import static org.assertj.core.api.Assertions.assertThat;
import org.elasticsearch.common.bytes.BytesArray;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.response.entity.TermVector;
import com.github.obourgain.elasticsearch.http.response.entity.TermVectorTest;

public class TermVectorResponseTest {

    @Test
    public void should_parse_response() throws Exception {
        String json = readFromClasspath("json/termvector/termvector_response.json");

        TermVectorResponse response = TermVectorResponse.doParse(new BytesArray(json));

        assertTermVectorResponse(response);
    }

    public static void assertTermVectorResponse(TermVectorResponse response) {
        assertThat(response.getId()).isEqualTo("1");
        assertThat(response.getIndex()).isEqualTo("twitter");
        assertThat(response.getType()).isEqualTo("tweet");
        assertThat(response.getVersion()).isEqualTo(1);

        TermVector termVector = response.getTermVector();
        TermVectorTest.assertTermVector(termVector);
        TermVectorTest.assertFieldStatistics(termVector);
    }

}