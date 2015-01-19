package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import org.assertj.core.api.Assertions;
import org.elasticsearch.client.Requests;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.admin.indices.delete.DeleteIndexResponse;

public class DeleteIndexActionHandlerTest extends AbstractTest {

    @Test
    public void should_delete_index() throws Exception {
        Assertions.assertThat(indexExists(THE_INDEX)).isTrue();

        DeleteIndexResponse response = httpClient.admin().indices().deleteIndex(Requests.deleteIndexRequest(THE_INDEX)).get();

        Assertions.assertThat(response.isAcknowledged()).isTrue();
        Assertions.assertThat(response.getError()).isNull();
        Assertions.assertThat(response.getStatus()).isEqualTo(200);
        Assertions.assertThat(indexExists(THE_INDEX)).isFalse();
    }

    @Test
    public void should_delete_all_indices_for_wildcard() throws Exception {
        Assertions.assertThat(indexExists(THE_INDEX)).isTrue();
        createIndex("test1");
        createIndex("test2");

//        ensureGreen(THE_INDEX, "test1", "test2");
//
//        // TODO merge other code and handle the 404
////        DeleteIndexResponse response = httpClient.admin().indices().deleteIndex(Requests.deleteIndexRequest("_all")).get();
//        DeleteIndexResponse response = httpClient.admin().indices().deleteIndex(Requests.deleteIndexRequest(THE_INDEX).indices("test1", "test2")).get();
//
//        Assertions.assertThat(response.isAcknowledged()).isTrue();
//        Assertions.assertThat(response.getError()).isNull();
//        Assertions.assertThat(response.getStatus()).isEqualTo(200);
//
//        Assertions.assertThat(indexExists(THE_INDEX)).isFalse();
//        Assertions.assertThat(indexExists("test1")).isFalse();
//        Assertions.assertThat(indexExists("test2")).isFalse();

        Assertions.assertThat(indexExists(THE_INDEX)).isTrue();

        DeleteIndexResponse response = httpClient.admin().indices().deleteIndex(Requests.deleteIndexRequest("_all")).get();

        Assertions.assertThat(response.isAcknowledged()).isTrue();
        Assertions.assertThat(response.getError()).isNull();
        Assertions.assertThat(response.getStatus()).isEqualTo(200);
        Assertions.assertThat(indexExists(THE_INDEX)).isFalse();
    }

    @Test
    public void should_fail_on_missing_index() throws Exception {
        Assertions.assertThat(indexExists(THE_INDEX)).isTrue();

        DeleteIndexResponse response = httpClient.admin().indices().deleteIndex(Requests.deleteIndexRequest("doesnotexists")).get();

        Assertions.assertThat(response.isAcknowledged()).isFalse();
        Assertions.assertThat(response.getError()).contains("IndexMissingException");
        Assertions.assertThat(response.getError()).contains(THE_INDEX);
        Assertions.assertThat(response.getStatus()).isEqualTo(404);
    }
}