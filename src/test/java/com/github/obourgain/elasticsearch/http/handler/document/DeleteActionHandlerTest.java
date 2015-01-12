package com.github.obourgain.elasticsearch.http.handler.document;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.VersionType;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.ElasticsearchHttpException;
import com.github.obourgain.elasticsearch.http.response.delete.DeleteResponse;

public class DeleteActionHandlerTest extends AbstractTest {

    @Test
    public void should_delete_document() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        IndexRequest request = Requests.indexRequest().index(THE_INDEX)
                .type(THE_TYPE)
                .id(THE_ID)
                .source(source.toBytes());
        transportClient.index(request).actionGet();

        DeleteResponse response = httpClient.delete(Requests.deleteRequest(THE_INDEX).type(THE_TYPE).id(THE_ID)).get();
        Assertions.assertThat(response.getIndex()).isEqualTo(THE_INDEX);
        Assertions.assertThat(response.getType()).isEqualTo(THE_TYPE);
        Assertions.assertThat(response.getId()).isEqualTo(THE_ID);
        Assertions.assertThat(response.getVersion()).isEqualTo(2);
        Assertions.assertThat(response.isFound()).isTrue();
    }

    @Test
    public void should_fail_for_different_versions() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        IndexRequest request = Requests.indexRequest().index(THE_INDEX)
                .type(THE_TYPE)
                .id(THE_ID)
                .version(1)
                .versionType(VersionType.EXTERNAL)
                .refresh(true)
                .source(source.toBytes());
        transportClient.index(request).actionGet();

        try {
            httpClient.delete(Requests.deleteRequest(THE_INDEX).type(THE_TYPE).id(THE_ID).version(2)).get();
        } catch (ExecutionException e) {
            Assertions.assertThat(e).hasCauseInstanceOf(ElasticsearchHttpException.class);
            Assertions.assertThat(e.getMessage()).contains("status code 409");
            Assertions.assertThat(e.getMessage()).contains("VersionConflictEngineException");
            Assertions.assertThat(e.getMessage()).contains("version conflict, current [1], provided [2]");
        }
    }
}