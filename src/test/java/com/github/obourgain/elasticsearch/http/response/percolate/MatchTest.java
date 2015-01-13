package com.github.obourgain.elasticsearch.http.response.percolate;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import static org.assertj.core.api.Assertions.*;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

public class MatchTest {

    @Test
    public void should_parse() throws Exception {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/response/percolate/match.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Match match = Match.parse(parser);

        assertMatch(match);
    }

    @Test
    public void should_parse_with_score() throws Exception {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/response/percolate/match_with_score.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Match match = Match.parse(parser);

        assertThat(match.getIndex()).isEqualTo("my-index");
        assertThat(match.getId()).isEqualTo("1");
        assertThat(match.getScore()).isEqualTo(1f);
    }

    public static void assertMatch(Match match) {
        assertThat(match.getIndex()).isEqualTo("my-index");
        assertThat(match.getId()).isEqualTo("1");
        assertThat(match.getScore()).isNull();
    }
}