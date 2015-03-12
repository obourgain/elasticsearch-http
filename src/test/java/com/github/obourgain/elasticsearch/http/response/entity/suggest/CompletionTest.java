package com.github.obourgain.elasticsearch.http.response.entity.suggest;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class CompletionTest {

    @Test
    public void should_parse_option() throws Exception {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/suggest/completion_option.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Completion.Option option = Completion.parseOption(parser);

        Assertions.assertThat(option.getText()).isEqualTo("Nirvana - Nevermind");
        Assertions.assertThat(option.getScore()).isEqualTo(34);
        Assertions.assertThat(option.getPayload()).isEqualTo("{\"artistId\":2321}");
    }

    @Test
    public void should_parse_options_array() throws Exception {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/suggest/completion_options_array.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        List<Completion.Option> options = Completion.parseOptions(parser);

        Assertions.assertThat(options).hasSize(2);

        Completion.Option option = options.get(0);
        Assertions.assertThat(option.getText()).isEqualTo("Death - Nothing is Everything");
        Assertions.assertThat(option.getScore()).isEqualTo(42);
        Assertions.assertThat(option.getPayload()).isNull();

        option = options.get(1);
        Assertions.assertThat(option.getText()).isEqualTo("Nirvana - Nevermind");
        Assertions.assertThat(option.getScore()).isEqualTo(34);
        Assertions.assertThat(option.getPayload()).isEqualTo("{\"artistId\":2321}");
    }

}