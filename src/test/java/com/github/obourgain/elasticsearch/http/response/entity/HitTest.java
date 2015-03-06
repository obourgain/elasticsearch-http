package com.github.obourgain.elasticsearch.http.response.entity;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import static net.javacrumbs.jsonunit.fluent.JsonFluentAssert.assertThatJson;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import java.io.IOException;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class HitTest {

    @Test
    public void should_parse_hit() throws IOException {
        String json = readFromClasspath("json/entity/hit.json");
        String source = readFromClasspath("json/entity/source.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Hit hit = new Hit().parse(parser);
        assertThat(hit.getId()).isEqualTo("1");
        assertThat(hit.getType()).isEqualTo("tweet");
        assertThat(hit.getIndex()).isEqualTo("twitter");
        assertThat(hit.getScore()).isEqualTo(1.7f, Offset.offset(0.01f));
        assertThatJson(new String(hit.getSource())).isEqualTo(source);
    }

    @Test
    public void should_parse_array_of_hits() throws IOException {
        String json = readFromClasspath("json/entity/hit_array.json");
        String source = readFromClasspath("json/entity/source.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        List<Hit> hits = Hit.parseHitArray(parser);

        assertThat(hits).hasSize(2);

        Hit hit = hits.get(0);
        assertThat(hit.getId()).isEqualTo("1");
        assertThat(hit.getType()).isEqualTo("tweet");
        assertThat(hit.getIndex()).isEqualTo("twitter");
        assertThat(hit.getScore()).isEqualTo(1.7f, Offset.offset(0.01f));
        assertThatJson(new String(hit.getSource())).isEqualTo(source);

        Hit hit2 = hits.get(1);
        assertThat(hit2.getId()).isEqualTo("2");
        assertThat(hit2.getType()).isEqualTo("tweet");
        assertThat(hit2.getIndex()).isEqualTo("twitter");
    }

    @Test
    public void should_parse_hit_with_sort() throws Exception {
        // beware, score is null is sorting and track_score is not set to true
        String json = readFromClasspath("json/entity/hit_with_sort.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Hit hit = new Hit().parse(parser);

        assertThat(hit.getId()).isEqualTo("2");
        assertThat(hit.getType()).isEqualTo("tweet");
        assertThat(hit.getIndex()).isEqualTo("twitter2");
        assertThat(hit.getScore()).isNull();
        assertThat(hit.getSource().length).isGreaterThan(1);

        assertThat(hit.getSort()).hasSize(1).containsOnly("12");

    }

    @Test
    public void should_parse_hit_with_fields() throws Exception {
        String json = readFromClasspath("json/entity/hit_with_fields.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Hit hit = new Hit().parse(parser);

        assertThat(hit.getId()).isEqualTo("1");
        assertThat(hit.getType()).isEqualTo("tweet");
        assertThat(hit.getIndex()).isEqualTo("twitter2");
        assertThat(hit.getScore()).isEqualTo(1, Offset.offset(0.01f));
        assertThat(hit.getSource()).isNull();

        assertThat(hit.getFields()).hasSize(2);

        assertThat(hit.getFields()).containsKey("message");
        assertThat(hit.getFields()).containsKey("the_int");

        assertThat(hit.getFields().get("message").getValues()).containsOnly("the message");
        assertThat(hit.getFields().get("the_int").getValues()).containsOnly("2");
    }

    @Test
    public void should_parse_hit_with_fields_with_multi_values() throws Exception {
        String json = readFromClasspath("json/entity/hit_with_fields_with_multi_values.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Hit hit = new Hit().parse(parser);

        assertThat(hit.getId()).isEqualTo("1");
        assertThat(hit.getType()).isEqualTo("tweet");
        assertThat(hit.getIndex()).isEqualTo("twitter2");
        assertThat(hit.getScore()).isEqualTo(1, Offset.offset(0.01f));
        assertThat(hit.getSource()).isNull();

        assertThat(hit.getFields()).hasSize(2);

        assertThat(hit.getFields()).containsKey("message");
        assertThat(hit.getFields()).containsKey("the_int");

        assertThat(hit.getFields().get("message").getValues()).containsOnly("the message");
        assertThat(hit.getFields().get("the_int").getValues()).containsOnly("2", "3", "4");
    }
}
