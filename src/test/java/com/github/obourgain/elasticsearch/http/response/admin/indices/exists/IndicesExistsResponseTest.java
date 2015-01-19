package com.github.obourgain.elasticsearch.http.response.admin.indices.exists;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;

public class IndicesExistsResponseTest extends AbstractTest {

    @Test
    public void should_return_true_when_exists() throws Exception {
        try {
            MILLISECONDS.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        IndicesExistsResponse existsResponse = httpClient.admin().indices().indexExists(new IndicesExistsRequest(THE_INDEX)).get();
        Assertions.assertThat(existsResponse.isExists()).isTrue();
    }

    @Test
    public void should_return_false_when_not_exists() throws Exception {
        IndicesExistsResponse existsResponse = httpClient.admin().indices().indexExists(new IndicesExistsRequest("foo")).get();
        Assertions.assertThat(existsResponse.isExists()).isFalse();
    }

    @Test
    public void should_return_false_when_asking_several_indices_and_one_does_not_exist() throws Exception {
        IndicesExistsResponse existsResponse = httpClient.admin().indices().indexExists(new IndicesExistsRequest("foo")).get();
        Assertions.assertThat(existsResponse.isExists()).isFalse();
    }

    @Test
    public void should_return_false_when_asking_several_indices_and_all_exists() throws Exception {
        createIndex("foo");
        createIndex("bar");
        ensureSearchable("foo", "bar");

        IndicesExistsResponse existsResponse = httpClient.admin().indices().indexExists(new IndicesExistsRequest("foo")).get();
        Assertions.assertThat(existsResponse.isExists()).isTrue();
    }
}