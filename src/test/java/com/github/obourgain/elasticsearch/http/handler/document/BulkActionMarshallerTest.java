package com.github.obourgain.elasticsearch.http.handler.document;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Arrays;
import java.util.Collections;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.junit.Test;

public class BulkActionMarshallerTest {

    @Test
    public void should_marshall_index_request() throws Exception {
        IndexRequest request = new IndexRequest();
        // TODO transportClient w/o source ?
        request.source("foo", "bar");
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        BulkActionMarshaller marshaller = new BulkActionMarshaller(Collections.<ActionRequest>singleton(request).iterator());

        byte[] action = marshaller.next();
        assertThat(action).isNotNull();
        String actionAsString = new String(action);
        assertThat(actionAsString).isEqualTo("{\"index\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"op_type\":\"index\"}}");

        assertThat(marshaller.nextRequestBody).isNotNull();

        byte[] body = marshaller.next();
        assertThat(body).isNotNull();
        assertThat(new String(body)).isEqualTo("{\"foo\":\"bar\"}");
        assertThat(marshaller.nextRequestBody).isNull();

        assertThat(marshaller.requests.hasNext()).isFalse();
    }

    @Test
    public void should_marshall_delete_request() throws Exception {
        DeleteRequest request = new DeleteRequest();
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        BulkActionMarshaller marshaller = new BulkActionMarshaller(Collections.<ActionRequest>singleton(request).iterator());

        byte[] action = marshaller.next();
        assertThat(action).isNotNull();
        String actionAsString = new String(action);
        assertThat(actionAsString).isEqualTo("{\"delete\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\"}}");

        assertThat(marshaller.nextRequestBody).isNull();
        assertThat(marshaller.requests.hasNext()).isFalse();

        byte[] body = marshaller.next();
        assertThat(body).isNull();
    }

    @Test
    public void should_marshall_update_request() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        request.doc("foo", "bar");
        BulkActionMarshaller marshaller = new BulkActionMarshaller(Collections.<ActionRequest>singleton(request).iterator());

        byte[] action = marshaller.next();
        assertThat(action).isNotNull();
        String actionAsString = new String(action);
        assertThat(actionAsString).isEqualTo("{\"update\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"_retry_on_conflict\":0}}");

        assertThat(marshaller.nextRequestBody).isNotNull();

        byte[] body = marshaller.next();
        assertThat(body).isNotNull();
        assertThat(new String(body)).isEqualTo("{\"doc\":{\"foo\":\"bar\"}}");
        assertThat(marshaller.nextRequestBody).isNull();
    }

    @Test
    public void should_marshall_update_request_with_upsert() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        request.upsert("bar", "baz");
        BulkActionMarshaller marshaller = new BulkActionMarshaller(Collections.<ActionRequest>singleton(request).iterator());

        byte[] action = marshaller.next();
        assertThat(action).isNotNull();
        String actionAsString = new String(action);
        assertThat(actionAsString).isEqualTo("{\"update\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"_retry_on_conflict\":0}}");

        assertThat(marshaller.nextRequestBody).isNotNull();

        byte[] body = marshaller.next();
        assertThat(body).isNotNull();
        assertThat(new String(body)).isEqualTo("{\"upsert\":{\"bar\":\"baz\"}}");
        assertThat(marshaller.nextRequestBody).isNull();
    }

    @Test
    public void should_marshall_update_request_with_doc_as_upsert() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        request.doc("bar", "baz");
        request.docAsUpsert(true);
        BulkActionMarshaller marshaller = new BulkActionMarshaller(Collections.<ActionRequest>singleton(request).iterator());

        byte[] action = marshaller.next();
        assertThat(action).isNotNull();
        String actionAsString = new String(action);
        assertThat(actionAsString).isEqualTo("{\"update\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"_retry_on_conflict\":0}}");

        assertThat(marshaller.nextRequestBody).isNotNull();

        byte[] body = marshaller.next();
        assertThat(body).isNotNull();
        assertThat(new String(body)).isEqualTo("{\"doc_as_upsert\":true,\"doc\":{\"bar\":\"baz\"}}");
        assertThat(marshaller.nextRequestBody).isNull();
    }

    @Test
    public void should_marshall_update_request_with_script() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        request.script("the_script");
        BulkActionMarshaller marshaller = new BulkActionMarshaller(Collections.<ActionRequest>singleton(request).iterator());

        byte[] action = marshaller.next();
        assertThat(action).isNotNull();
        String actionAsString = new String(action);
        assertThat(actionAsString).isEqualTo("{\"update\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"_retry_on_conflict\":0}}");

        assertThat(marshaller.nextRequestBody).isNotNull();

        byte[] body = marshaller.next();
        assertThat(body).isNotNull();
        assertThat(new String(body)).isEqualTo("{\"script\":\"the_script\"}");
        assertThat(marshaller.nextRequestBody).isNull();
    }

    @Test
    public void should_marshall_update_request_with_script_and_upsert() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        request.script("the_script");
        request.upsert("bar", "baz");
        BulkActionMarshaller marshaller = new BulkActionMarshaller(Collections.<ActionRequest>singleton(request).iterator());

        byte[] action = marshaller.next();
        assertThat(action).isNotNull();
        String actionAsString = new String(action);
        assertThat(actionAsString).isEqualTo("{\"update\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"_retry_on_conflict\":0}}");

        assertThat(marshaller.nextRequestBody).isNotNull();

        byte[] body = marshaller.next();
        assertThat(body).isNotNull();
        assertThat(new String(body)).isEqualTo("{\"upsert\":{\"bar\":\"baz\"},\"script\":\"the_script\"}");
        assertThat(marshaller.nextRequestBody).isNull();
    }

    @Test
    public void should_marshall_several_actions() throws Exception {
        UpdateRequest update = new UpdateRequest();
        update.index("the_index");
        update.type("the_type");
        update.id("the_id");
        update.script("the_script");
        update.upsert("bar", "baz");

        DeleteRequest delete = new DeleteRequest();
        delete.index("the_index");
        delete.type("the_type");
        delete.id("the_id");

        IndexRequest index = new IndexRequest();
        index.source("foo", "bar");
        index.index("the_index");
        index.type("the_type");
        index.id("the_id");

        BulkActionMarshaller marshaller = new BulkActionMarshaller(Arrays.asList(new ActionRequest[]{update, delete, index}).iterator());

        // update request
        byte[] action = marshaller.next();
        assertThat(action).isNotNull();
        assertThat(new String(action)).isEqualTo("{\"update\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"_retry_on_conflict\":0}}");
        assertThat(marshaller.nextRequestBody).isNotNull();

        // update body
        byte[] body = marshaller.next();
        assertThat(body).isNotNull();
        assertThat(new String(body)).isEqualTo("{\"upsert\":{\"bar\":\"baz\"},\"script\":\"the_script\"}");
        assertThat(marshaller.nextRequestBody).isNull();

        // delete request
        action = marshaller.next();
        assertThat(action).isNotNull();
        assertThat(new String(action)).isEqualTo("{\"delete\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\"}}");
        assertThat(marshaller.nextRequestBody).isNull();

        // index request
        action = marshaller.next();
        assertThat(action).isNotNull();
        assertThat(new String(action)).isEqualTo("{\"index\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"op_type\":\"index\"}}");
        assertThat(marshaller.nextRequestBody).isNotNull();

        // index body
        body = marshaller.next();
        assertThat(body).isNotNull();
        assertThat(new String(body)).isEqualTo("{\"foo\":\"bar\"}");
        assertThat(marshaller.nextRequestBody).isNull();

        assertThat(marshaller.next()).isNull();
    }

    public BulkActionMarshallerTest() {
        super();
    }
}