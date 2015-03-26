package com.github.obourgain.elasticsearch.http.handler.document;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.ElasticsearchHttpException;
import com.github.obourgain.elasticsearch.http.handler.document.get.GetResponse;

public class GetActionHandlerTest extends AbstractTest {

    @Test
    public void should_get_document() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        byte[] array = source.toBytes();
        IndexRequest request = Requests.indexRequest().index(THE_INDEX)
                .type(THE_TYPE)
                .id(THE_ID)
                .source(array);
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);

        transportClient.index(request).actionGet();

        GetResponse getResponse = httpClient.get(Requests.getRequest(THE_INDEX).type(THE_TYPE).id(THE_ID)
                .fetchSourceContext(FetchSourceContext.FETCH_SOURCE))
                .get();

        compareMap(expected, getResponse.getSource());
    }

    @Test
    public void should_get_document_without_source() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        byte[] array = source.toBytes();
        IndexRequest request = Requests.indexRequest().index(THE_INDEX)
                .type(THE_TYPE)
                .id(THE_ID)
                .source(array);

        transportClient.index(request).actionGet();

        GetResponse getResponse = httpClient.get(Requests.getRequest(THE_INDEX).type(THE_TYPE).id(THE_ID)
                .fields("the_string_field")
                .fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE))
                .get();

        Assertions.assertThat(getResponse.getSource()).isNull();
        Assertions.assertThat(getResponse.getFields()).hasSize(1)
                .containsKey("the_string_field");
    }

    @Test
    public void should_get_document_fields() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        byte[] array = source.toBytes();
        IndexRequest request = Requests.indexRequest().index(THE_INDEX)
                .type(THE_TYPE)
                .id(THE_ID)
                .source(array);

        transportClient.index(request).actionGet();

        GetResponse getResponse = httpClient.get(Requests.getRequest(THE_INDEX).type(THE_TYPE).id(THE_ID)
                .fields("the_string_field"))
                .get();

        Assertions.assertThat(getResponse.getSource()).isNull();
        Assertions.assertThat(getResponse.getFields()).hasSize(1)
                .containsKey("the_string_field");
    }

    @Test
    public void should_get_document_version() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        byte[] array = source.toBytes();
        IndexRequest request = Requests.indexRequest().index(THE_INDEX)
                .type(THE_TYPE)
                .id(THE_ID)
                .source(array);

        transportClient.index(request).actionGet();
        transportClient.index(request).actionGet();

        httpClient.get(Requests.getRequest(THE_INDEX).type(THE_TYPE).id(THE_ID)
                .version(2))
                .get();
    }

    @Test
    public void should_fail_when_version_different() throws IOException, InterruptedException {
        BytesReference source = source().bytes();
        byte[] array = source.toBytes();
        IndexRequest request = Requests.indexRequest().index(THE_INDEX)
                .type(THE_TYPE)
                .id(THE_ID)
                .source(array);

        transportClient.index(request).actionGet();
        transportClient.index(request).actionGet();

        try {
            httpClient.get(Requests.getRequest(THE_INDEX).type(THE_TYPE).id(THE_ID)
                    .version(1))
                    .get();
        } catch (ExecutionException e) {
            Assertions.assertThat(e).hasCauseInstanceOf(ElasticsearchHttpException.class);
            Assertions.assertThat(e.getMessage()).contains("status code 409");
            Assertions.assertThat(e.getMessage()).contains("VersionConflictEngineException");
            Assertions.assertThat(e.getMessage()).contains("version conflict, current [2], provided [1]");
        }
    }

}