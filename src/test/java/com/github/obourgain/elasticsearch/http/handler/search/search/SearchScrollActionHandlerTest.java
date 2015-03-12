package com.github.obourgain.elasticsearch.http.handler.search.search;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;

public class SearchScrollActionHandlerTest extends AbstractTest {

    private static List<String> scrollIds = Collections.synchronizedList(new ArrayList<String>());

    @After
    public void clear_scrolls() {
        if(!scrollIds.isEmpty()) {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.scrollIds(scrollIds);
            transportClient.clearScroll(clearScrollRequest).actionGet();
            scrollIds.clear();
        }
    }

    @Test
    public void should_scroll() throws Exception {
        for (int i = 0; i < 10; i++) {
            createSimpleDoc(THE_INDEX, THE_TYPE, String.valueOf(i));
        }
        ensureSearchable(THE_INDEX);

        SearchResponse searchResponse = httpClient.search(new SearchRequest().source(new SearchSourceBuilder().query(matchAllQuery()).size(2)).scroll("5m")).get();

        String scrollId = searchResponse.getScrollId();
        addScrollIdToClear(searchResponse);
        Assertions.assertThat(scrollId).isNotNull();
        Assertions.assertThat(searchResponse.getHits().getTotal()).isEqualTo(10);

        for (int i = 0; i < 3; i++) {
            try {
                SearchResponse scrollResponse = httpClient.searchScroll(new SearchScrollRequest(scrollId).scroll("5m")).get();

                scrollId = scrollResponse.getScrollId();
                addScrollIdToClear(searchResponse);
                Assertions.assertThat(scrollResponse.getHits().getTotal()).isEqualTo(10);
                Assertions.assertThat(scrollResponse.getHits().getHits()).hasSize(2);
            } catch (Throwable e) {
                throw new AssertionError("failed at query number " + i + " with : ", e);
            }
        }

        SearchResponse scrollResponse = httpClient.searchScroll(new SearchScrollRequest(scrollId)).get();
        addScrollIdToClear(searchResponse); // should not be one, but clean up stuff is ever ...
        Assertions.assertThat(scrollResponse.getHits().getTotal()).isEqualTo(10);
        Assertions.assertThat(scrollResponse.getHits().getHits()).hasSize(2);
    }

    @Test
    public void should_fail_when_scroll_id_is_missing() throws Exception {
        try {
            httpClient.searchScroll(new SearchScrollRequest()).get();
            fail();
        } catch (ExecutionException e) {
            Assertions.assertThat(e).hasCauseInstanceOf(ActionRequestValidationException.class);
            Assertions.assertThat(e.getCause()).hasMessageContaining("scrollId is missing");
        }
    }

    private void addScrollIdToClear(SearchResponse searchResponse) {
        String scrollId = searchResponse.getScrollId();
        if (scrollId != null) {
            scrollIds.add(scrollId);
        }
    }
}