package com.github.obourgain.elasticsearch.http.response.entity;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import static net.javacrumbs.jsonunit.fluent.JsonFluentAssert.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.*;
import java.io.IOException;
import java.util.List;
import org.assertj.core.data.Offset;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class HitsTest {

    @Test
    public void should_parse_hits() throws IOException {
        String json = readFromClasspath("json/entity/hits.json");
        String source = readFromClasspath("json/entity/source.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Hits hits = new Hits().parse(parser);

        assertThat(hits.getMaxScore()).isEqualTo(1.7f, offset(0.01f));
        assertThat(hits.getTotal()).isEqualTo(2);

        assertThat(hits.getHits()).hasSize(2);

        Hit hit = hits.getHits().get(0);
        assertThat(hit.getId()).isEqualTo("1");
        assertThat(hit.getType()).isEqualTo("tweet");
        assertThat(hit.getIndex()).isEqualTo("twitter");
        assertThat(hit.getScore()).isEqualTo(1.7f, offset(0.01f));
        assertThatJson(new String(hit.getSource())).isEqualTo(source);
    }

}
