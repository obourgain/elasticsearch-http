package com.github.obourgain.elasticsearch.http.response.entity.suggest;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import org.assertj.core.api.Assertions;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class PhraseTest {

    @Test
    public void should_parse_option() throws Exception {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/suggest/phrase_option.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Phrase.Option option = Phrase.parseOption(parser);

        Assertions.assertThat(option.getText()).isEqualTo("xorr the god jewel");
        Assertions.assertThat(option.getScore()).isEqualTo(0.17877324f);
        Assertions.assertThat(option.getCollate()).isNull();
        Assertions.assertThat(option.getHighlighted()).isEqualTo("<em>xorr</em> the <em>god</em> jewel");
    }
}