package com.github.obourgain.elasticsearch.http.response.admin.indices.mapping.get;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.elasticsearch.common.bytes.BytesArray;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.TestFilesUtils;
import com.github.obourgain.elasticsearch.http.handler.search.search.SearchResponse;
import com.github.obourgain.elasticsearch.http.response.entity.Hit;

public class GetMappingsResponseTest {

    @Test
    public void should_parse_response() throws Exception {
        String json = TestFilesUtils.readFromClasspath("com/github/obourgain/elasticsearch/http/handler/mappings/get/get_mapping.json");

        SearchResponse searchResponse = SearchResponse.doParse(new BytesArray(json));

        assertThat(searchResponse.getShards().getTotal()).isEqualTo(20);
        assertThat(searchResponse.getShards().getSuccessful()).isEqualTo(20);
        assertThat(searchResponse.getShards().getFailed()).isEqualTo(0);

        assertThat(searchResponse.getScrollId()).isNull();
        assertThat(searchResponse.getTookInMillis()).isEqualTo(11);
        assertThat(searchResponse.isTimedOut()).isFalse();

        assertThat(searchResponse.getHits().getTotal()).isEqualTo(1);
        assertThat(searchResponse.getHits().getMaxScore()).isEqualTo(1);

        List<Hit> hits = searchResponse.getHits().getHits();
        assertThat(hits).hasSize(1);
        assertThat(hits.get(0).getIndex()).isEqualTo("the_index");
        assertThat(hits.get(0).getType()).isEqualTo("the_type");
        assertThat(hits.get(0).getId()).isEqualTo("the_id");
        assertThat(hits.get(0).getScore()).isEqualTo(1);
        assertThat(hits.get(0).getSource().length).isGreaterThan(1);
    }

}