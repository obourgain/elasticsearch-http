package com.github.obourgain.elasticsearch.http.response.search.percolate;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import static org.assertj.core.api.Assertions.assertThat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.handler.search.percolate.Match;
import com.github.obourgain.elasticsearch.http.handler.search.percolate.Matches;

public class MatchesTest {

    @Test
    public void should_parse() throws Exception {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/response/search/percolate/matches.json");

        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        Matches matches = Matches.parse(parser);

        assertMatches(matches);
    }

    public static void assertMatches(Matches matches) {
        assertThat(matches.getMatches()).hasSize(2);

        Match match1 = matches.getMatches().get(0);
        MatchTest.assertMatch(match1);

        Match match2 = matches.getMatches().get(1);
        assertThat(match2.getIndex()).isEqualTo("my-index");
        assertThat(match2.getId()).isEqualTo("2");
        assertThat(match2.getScore()).isNull();
    }
}