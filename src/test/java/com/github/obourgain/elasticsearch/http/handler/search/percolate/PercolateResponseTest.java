package com.github.obourgain.elasticsearch.http.handler.search.percolate;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import static com.github.obourgain.elasticsearch.http.response.search.percolate.MatchesTest.*;
import static org.assertj.core.api.Assertions.*;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.handler.search.percolate.PercolateResponse;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;

public class PercolateResponseTest {

    @Test
    public void should_parse() throws Exception {
        String json = readFromClasspath("com/github/obourgain/elasticsearch/http/response/search/percolate/percolate_response.json");
        XContentParser parser = XContentHelper.createParser(json.getBytes(), 0, json.length());
        parser.nextToken();

        PercolateResponse response = new PercolateResponse().parse(new BytesArray(json.getBytes()));

        Shards shards = response.getShards();
        assertThat(shards.getTotal()).isEqualTo(5);
        assertThat(shards.getSuccessful()).isEqualTo(5);
        assertThat(shards.getFailed()).isEqualTo(0);

        assertMatches(response.getMatches());
    }
}