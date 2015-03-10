package com.github.obourgain.elasticsearch.http.response.entity;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class ShardSearchFailureTest {

    @Test
    public void should_parse_failure() throws Exception {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/failure/shard_failure.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        ShardSearchFailure failure = new ShardSearchFailure().doParse(parser);

        Assertions.assertThat(failure.getIndex()).isNull();
        Assertions.assertThat(failure.getShard()).isNull();
        Assertions.assertThat(failure.getStatus()).isEqualTo(404);
        Assertions.assertThat(failure.getReason()).isEqualTo("SearchContextMissingException[No search context found for id [63]]");
    }

    @Test
    public void should_parse_failures_array() throws Exception {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/failure/shard_failure_array.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        List<ShardSearchFailure> failures = ShardSearchFailure.parse(parser);

        Assertions.assertThat(failures).hasSize(3);

        ShardSearchFailure failure = failures.get(0);
        Assertions.assertThat(failure.getIndex()).isNull();
        Assertions.assertThat(failure.getShard()).isNull();
        Assertions.assertThat(failure.getStatus()).isEqualTo(404);
        Assertions.assertThat(failure.getReason()).isEqualTo("SearchContextMissingException[No search context found for id [63]]");

        failure = failures.get(1);
        Assertions.assertThat(failure.getIndex()).isNull();
        Assertions.assertThat(failure.getShard()).isNull();
        Assertions.assertThat(failure.getStatus()).isEqualTo(404);
        Assertions.assertThat(failure.getReason()).isEqualTo("SearchContextMissingException[No search context found for id [64]]");

        failure = failures.get(2);
        Assertions.assertThat(failure.getIndex()).isNull();
        Assertions.assertThat(failure.getShard()).isNull();
        Assertions.assertThat(failure.getStatus()).isEqualTo(404);
        Assertions.assertThat(failure.getReason()).isEqualTo("SearchContextMissingException[No search context found for id [65]]");
    }
}