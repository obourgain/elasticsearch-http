package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import static java.lang.Long.MAX_VALUE;
import java.util.Collections;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.admin.indices.refresh.RefreshResponse;

public class RefreshActionHandlerTest extends AbstractTest {

    @Test
    public void should_refresh_index() throws Exception {
        transportClient.admin().indices()
                .updateSettings(Requests.updateSettingsRequest(THE_INDEX)
                        .settings(Collections.singletonMap("refresh_interval", MAX_VALUE)));

        index(THE_INDEX, THE_TYPE, THE_ID, "foo", "bar");

        SearchResponse searchResponse = transportClient.search(new SearchRequest(THE_INDEX).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))).actionGet();
        Assertions.assertThat(searchResponse.getHits().getTotalHits()).isEqualTo(0);

        RefreshResponse refreshResponse = httpClient.admin().indices().refresh(new RefreshRequest(THE_INDEX)).get();

        NumShards actualNumShards = getNumShards(THE_INDEX);
        Assertions.assertThat(refreshResponse.getShards().getTotal()).isEqualTo(actualNumShards.totalNumShards);

        searchResponse = transportClient.search(new SearchRequest(THE_INDEX).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()))).actionGet();
        Assertions.assertThat(searchResponse.getHits().getTotalHits()).isEqualTo(1);
    }
}