package com.github.obourgain.elasticsearch.http.response.entity.suggest;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class TermTest {

    @Test
    public void should_parse_option() throws Exception {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/suggest/term_option.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Term.Option option = Term.parseOption(parser);

        Assertions.assertThat(option.getText()).isEqualTo("everything");
        Assertions.assertThat(option.getScore()).isEqualTo(0.9f);
        Assertions.assertThat(option.getFreq()).isEqualTo(1);
    }
}