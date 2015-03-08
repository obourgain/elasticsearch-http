package com.github.obourgain.elasticsearch.http.response.entity;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;

public class ExplanationTest {

    @Test
    public void should_parse_explanation() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/search/explain/explanation.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Explanation explanation = new Explanation().parse(parser);

        assertThat(explanation.getDescription()).isEqualTo("termFreq=1.0");
        assertThat(explanation.getValue()).isEqualTo(1.0f);
        assertThat(explanation.getDetails()).isEmpty();
    }

    @Test
    public void should_parse_array() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/search/explain/explanation_array.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        List<Explanation> explanations = Explanation.parseArray(parser);

        assertThat(explanations).hasSize(3);

        Explanation explanation1 = explanations.get(0);
        assertThat(explanation1.getDescription()).isEqualTo("tf(freq=1.0), with freq of:");
        assertThat(explanation1.getValue()).isEqualTo(1.0f);
        assertThat(explanation1.getDetails()).hasSize(1);

        Explanation explanation2 = explanations.get(1);
        assertThat(explanation2.getDescription()).isEqualTo("idf(docFreq=1, maxDocs=1)");
        assertThat(explanation2.getValue()).isEqualTo(0.30685282f);
        assertThat(explanation2.getDetails()).isEmpty();

        Explanation explanation3 = explanations.get(2);
        assertThat(explanation3.getDescription()).isEqualTo("fieldNorm(doc=0)");
        assertThat(explanation3.getValue()).isEqualTo(0.5f);
        assertThat(explanation3.getDetails()).isEmpty();
    }

    @Test
    public void should_parse_explanation_with_details() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/response/search/explain/explanation_with_details.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Explanation explanation = new Explanation().parse(parser);

        assertThat(explanation.getDescription()).isEqualTo("fieldWeight in 0, product of:");
        assertThat(explanation.getValue()).isEqualTo(0.15342641f);

        List<Explanation> details = explanation.getDetails();
        assertThat(details).hasSize(3);

        Explanation explanation1 = details.get(0);
        assertThat(explanation1.getDescription()).isEqualTo("tf(freq=1.0), with freq of:");
        assertThat(explanation1.getValue()).isEqualTo(1.0f);
        assertThat(explanation1.getDetails()).hasSize(1);

        Explanation explanation2 = details.get(1);
        assertThat(explanation2.getDescription()).isEqualTo("idf(docFreq=1, maxDocs=1)");
        assertThat(explanation2.getValue()).isEqualTo(0.30685282f);
        assertThat(explanation2.getDetails()).isEmpty();

        Explanation explanation3 = details.get(2);
        assertThat(explanation3.getDescription()).isEqualTo("fieldNorm(doc=0)");
        assertThat(explanation3.getValue()).isEqualTo(0.5f);
        assertThat(explanation3.getDetails()).isEmpty();
    }
}