package com.github.obourgain.elasticsearch.http.handler.search.search;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import java.io.IOException;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class MultiSearchResponseTest {

    @Test
    public void should_parse_response() throws IOException {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/handler/search/search/multisearch_response.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        MultiSearchResponse response = new MultiSearchResponse().doParse(parser);

        List<SearchResponse> responses = response.getResponses();
        Assertions.assertThat(responses).hasSize(5);
        Assertions.assertThat(responses.get(0).getHits().getTotal()).isEqualTo(2);

        Assertions.assertThat(responses.get(1).getHits().getTotal()).isEqualTo(5);
        Assertions.assertThat(responses.get(1).getHits().getMaxScore()).isEqualTo(1.2f);
        Assertions.assertThat(responses.get(1).getHits().getHits()).isEmpty();

        Assertions.assertThat(responses.get(3).getHits().getTotal()).isEqualTo(2);
        Assertions.assertThat(responses.get(3).getHits().getMaxScore()).isEqualTo(1.0f);
        Assertions.assertThat(responses.get(3).getHits().getHits()).isNotNull();

        Assertions.assertThat(responses.get(4).getHits().getTotal()).isEqualTo(0);
    }
}