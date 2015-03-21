package com.github.obourgain.elasticsearch.http.response.entity;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.Map;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class HighlightTest {

    @Test
    public void should_parse_highlight() throws Exception {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/highlight/highlight.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        parser.nextToken(); // skip the start object

        Highlight highlight = new Highlight().parse(parser);

        assertThat(highlight.getValue()).isEqualTo("The quick <em>brown</em> <em>fox</em> jumps over the lazy dog");
    }

    @Test
    public void should_parse_highlights() throws Exception {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/response/entity/highlight/highlights.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();
        parser.nextToken(); // set the parser's current name to highlight to satisfy assertions
        parser.nextToken(); // move to start object

        Map<String, Highlight> highlights = Highlight.parseHighlights(parser);

        assertThat(highlights).hasSize(1).containsKey("body");
        assertThat(highlights.get("body").getValue()).isEqualTo("The quick <em>brown</em> <em>fox</em> jumps over the lazy dog");
    }

}