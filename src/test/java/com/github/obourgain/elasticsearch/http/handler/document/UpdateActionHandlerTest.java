package com.github.obourgain.elasticsearch.http.handler.document;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.MapEntry;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.lookup.SourceLookup;
import org.junit.Ignore;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.ElasticsearchHttpException;
import com.github.obourgain.elasticsearch.http.handler.document.update.UpdateResponse;

public class UpdateActionHandlerTest extends AbstractTest {

    @Test
    public void should_update_document_with_doc() throws Exception {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);
        index(THE_INDEX, THE_TYPE, THE_ID, expected);

        UpdateResponse updateResponse = httpClient.update(new UpdateRequest(THE_INDEX, THE_TYPE, THE_ID).doc(source.toBytes())).get();
        Assertions.assertThat(updateResponse.getIndex()).isEqualTo(THE_INDEX);
        Assertions.assertThat(updateResponse.getType()).isEqualTo(THE_TYPE);
        Assertions.assertThat(updateResponse.getId()).isEqualTo(THE_ID);
        Assertions.assertThat(updateResponse.getVersion()).isEqualTo(2);
        Assertions.assertThat(updateResponse.isCreated()).isFalse();
    }

    @Test
    public void should_upsert() throws Exception {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);

        UpdateResponse updateResponse = httpClient.update(new UpdateRequest(THE_INDEX, THE_TYPE, THE_ID)
                .doc(source.toBytes())
                .upsert(source.toBytes())
        ).get();

        Assertions.assertThat(updateResponse.getIndex()).isEqualTo(THE_INDEX);
        Assertions.assertThat(updateResponse.getType()).isEqualTo(THE_TYPE);
        Assertions.assertThat(updateResponse.getId()).isEqualTo(THE_ID);
        Assertions.assertThat(updateResponse.getVersion()).isEqualTo(1);
        Assertions.assertThat(updateResponse.isCreated()).isTrue();

        Map<String, Object> actualSource = get(THE_INDEX, THE_TYPE, THE_ID).getSource();
        compareMap(expected, actualSource);
    }

    @Test
    public void should_use_doc_as_upsert() throws Exception {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);

        UpdateRequest updateRequest = new UpdateRequest(THE_INDEX, THE_TYPE, THE_ID).doc(source.toBytes());
        updateRequest.docAsUpsert(true);
        UpdateResponse updateResponse = httpClient.update(updateRequest).get();

        Assertions.assertThat(updateResponse.getIndex()).isEqualTo(THE_INDEX);
        Assertions.assertThat(updateResponse.getType()).isEqualTo(THE_TYPE);
        Assertions.assertThat(updateResponse.getId()).isEqualTo(THE_ID);
        Assertions.assertThat(updateResponse.getVersion()).isEqualTo(1);
        Assertions.assertThat(updateResponse.isCreated()).isTrue();

        Map<String, Object> actualSource = get(THE_INDEX, THE_TYPE, THE_ID).getSource();
        compareMap(expected, actualSource);
    }

    @Test
    @Ignore("ElasticsearchIllegalArgumentException[script_lang not supported [groovy]]")
    public void should_update_document_with_script() throws Exception {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);
        index(THE_INDEX, THE_TYPE, THE_ID, expected);

        UpdateResponse updateResponse = httpClient.update(new UpdateRequest(THE_INDEX, THE_TYPE, THE_ID)
                .script("ctx._source.new_fields = \"value\"")).get();
        Assertions.assertThat(updateResponse.getIndex()).isEqualTo(THE_INDEX);
        Assertions.assertThat(updateResponse.getType()).isEqualTo(THE_TYPE);
        Assertions.assertThat(updateResponse.getId()).isEqualTo(THE_ID);
        Assertions.assertThat(updateResponse.getVersion()).isEqualTo(2);
        Assertions.assertThat(updateResponse.isCreated()).isFalse();

        Map<String, Object> actualSource = get(THE_INDEX, THE_TYPE, THE_ID).getSource();
        Assertions.assertThat(actualSource).contains(MapEntry.entry("actualSource", "value"));
    }

    @Test
    public void should_not_update_document_when_version_does_not_match() throws Exception {
        BytesReference source = source().bytes();
        index(THE_INDEX, THE_TYPE, THE_ID, SourceLookup.sourceAsMap(source));

        try {
            httpClient.update(new UpdateRequest(THE_INDEX, THE_TYPE, THE_ID).doc(source.toBytes()).version(3)).get();
            fail();
        } catch (ExecutionException e) {
            Assertions.assertThat(e).hasCauseInstanceOf(ElasticsearchHttpException.class);
            Assertions.assertThat(e.getCause()).hasMessageStartingWith("status code 409");
            Assertions.assertThat(e.getCause()).hasMessageContaining("VersionConflictEngineException");
        }
    }

}