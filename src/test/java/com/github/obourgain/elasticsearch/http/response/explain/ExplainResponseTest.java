package com.github.obourgain.elasticsearch.http.response.explain;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;
import com.github.obourgain.elasticsearch.http.response.entity.Explanation;

public class ExplainResponseTest {

    @Test
    public void should_parse() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/explain/explain_response.json");

        ExplainResponse response = ExplainResponse.doParse(json.getBytes());

        assertThat(response.getIndex()).isEqualTo("twitter");
        assertThat(response.getType()).isEqualTo("tweet");
        assertThat(response.getId()).isEqualTo("1");
        assertThat(response.isMatched()).isTrue();

        Explanation explanation = response.getExplanation();
        assertThat(explanation.getValue()).isEqualTo(0.15342641f);
        assertThat(explanation.getDescription()).isEqualTo("weight(message:out in 0) [PerFieldSimilarity], result of:");
        assertThat(explanation.getDetails()).hasSize(1);
    }

    @Test
    public void should_parse_non_matched() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/explain/explain_response_not_matched.json");

        ExplainResponse response = ExplainResponse.doParse(json.getBytes());

        assertThat(response.getIndex()).isEqualTo("twitter");
        assertThat(response.getType()).isEqualTo("tweet");
        assertThat(response.getId()).isEqualTo("1");
        assertThat(response.isMatched()).isFalse();

        assertThat(response.getExplanation().getValue()).isEqualTo(0);
        assertThat(response.getExplanation().getDescription()).isEqualTo("no matching term");
    }
}