package com.github.obourgain.elasticsearch.http.handler.document;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.handler.document.multiget.MultiGetResponse;
import com.github.obourgain.elasticsearch.http.handler.document.multiget.MultiGetResponseError;

public class MultiGetActionHandlerTest extends AbstractTest {

    @Test
    public void should_get_document() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);
        createDoc(THE_INDEX, THE_TYPE, THE_ID, source.toUtf8());

        MultiGetRequest multiGetRequest = new MultiGetRequest().add(THE_INDEX, THE_TYPE, THE_ID);
        MultiGetResponse response = httpClient.multiGet(multiGetRequest).get();

        Assertions.assertThat(response.all()).hasSize(1);
        Assertions.assertThat(response.errors()).hasSize(0);

        compareMap(expected, response.documents().get(0).getSource());
    }

    @Test
    public void should_get_several_documents() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();

        createDoc(THE_INDEX, THE_TYPE, THE_ID, source.toUtf8());
        createDoc(THE_INDEX, THE_TYPE, "2", source.toUtf8());
        createDoc(THE_INDEX, THE_TYPE, "3", source.toUtf8());

        MultiGetRequest multiGetRequest = new MultiGetRequest()
                .add(THE_INDEX, THE_TYPE, THE_ID)
                .add(THE_INDEX, THE_TYPE, "2")
                .add(THE_INDEX, THE_TYPE, "3");
        MultiGetResponse response = httpClient.multiGet(multiGetRequest).get();

        Assertions.assertThat(response.all()).hasSize(3);
        Assertions.assertThat(response.errors()).hasSize(0);

        Assertions.assertThat(response.documents().get(0).getId()).isEqualTo(THE_ID);
        Assertions.assertThat(response.documents().get(1).getId()).isEqualTo("2");
        Assertions.assertThat(response.documents().get(2).getId()).isEqualTo("3");
    }

    @Test
    public void should_get_documents_without_source() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();

        createDoc(THE_INDEX, THE_TYPE, THE_ID, source.toUtf8());

        MultiGetRequest.Item item = new MultiGetRequest.Item(THE_INDEX, THE_TYPE, THE_ID).fields("the_string_field").fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        MultiGetRequest multiGetRequest = new MultiGetRequest().add(item);
        MultiGetResponse response = httpClient.multiGet(multiGetRequest).get();

        Assertions.assertThat(response.all()).hasSize(1);
        Assertions.assertThat(response.errors()).hasSize(0);

        Assertions.assertThat(response.documents().get(0).getSource()).isNull();
        Assertions.assertThat(response.documents().get(0).getFields()).hasSize(1).containsKey("the_string_field");
    }

    @Test
    public void should_get_document_version() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();

        createDoc(THE_INDEX, THE_TYPE, THE_ID, source.toUtf8());
        createDoc(THE_INDEX, THE_TYPE, THE_ID, source.toUtf8());

        MultiGetRequest.Item item = new MultiGetRequest.Item(THE_INDEX, THE_TYPE, THE_ID)
                .version(2)
                .fields("the_string_field")
                .fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        MultiGetRequest multiGetRequest = new MultiGetRequest().add(item);
        MultiGetResponse response = httpClient.multiGet(multiGetRequest).get();

        Assertions.assertThat(response.all()).hasSize(1);
        Assertions.assertThat(response.errors()).hasSize(0);

        Assertions.assertThat(response.documents().get(0).getVersion()).isEqualTo(2);
    }

    @Test
    public void should_fail_when_version_different() throws IOException, InterruptedException, ExecutionException {
        BytesReference source = source().bytes();

        createDoc(THE_INDEX, THE_TYPE, THE_ID, source.toUtf8());
        createDoc(THE_INDEX, THE_TYPE, THE_ID, source.toUtf8());

        MultiGetRequest.Item item = new MultiGetRequest.Item(THE_INDEX, THE_TYPE, THE_ID)
                .version(1)
                .fields("the_string_field")
                .fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        MultiGetRequest multiGetRequest = new MultiGetRequest().add(item);
        MultiGetResponse response = httpClient.multiGet(multiGetRequest).get();

        Assertions.assertThat(response.all()).hasSize(1);
        List<MultiGetResponseError> errors = response.errors();
        Assertions.assertThat(errors).hasSize(1);

        Assertions.assertThat(errors.get(0).getIndex()).isEqualTo(THE_INDEX);
        Assertions.assertThat(errors.get(0).getType()).isEqualTo(THE_TYPE);
        Assertions.assertThat(errors.get(0).getId()).isEqualTo(THE_ID);
        Assertions.assertThat(errors.get(0).getError()).contains("VersionConflictEngineException").contains("version conflict, current [2], provided [1]]");
    }

}