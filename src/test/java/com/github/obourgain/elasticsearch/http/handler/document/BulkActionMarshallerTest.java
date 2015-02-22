package com.github.obourgain.elasticsearch.http.handler.document;

import static com.github.obourgain.elasticsearch.http.ObservableAsserts.assertHasSize;
import static com.github.obourgain.elasticsearch.http.ObservableAsserts.takeNth;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.Arrays;
import java.util.Collections;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.handler.document.bulk.BulkActionMarshaller;
import rx.Observable;

public class BulkActionMarshallerTest {

    @Test
    public void should_marshall_index_request() throws Exception {
        IndexRequest request = new IndexRequest();
        // TODO transportClient w/o source ?
        request.source("foo", "bar");
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        Observable<byte[]> observable = BulkActionMarshaller.lazyConvertToBytes(Collections.<ActionRequest>singletonList(request));

        assertHasSize(observable, 4);

        byte[] bytes = takeNth(observable, 0);
        assertThat(new String(bytes)).isEqualTo("{\"index\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"op_type\":\"index\"}}");

        bytes = takeNth(observable, 1);
        assertThat(new String(bytes)).isEqualTo("\n");

        bytes = takeNth(observable, 2);
        assertThat(new String(bytes)).isEqualTo("{\"foo\":\"bar\"}");

        bytes = takeNth(observable, 3);
        assertThat(new String(bytes)).isEqualTo("\n");
    }

    @Test
    public void should_marshall_delete_request() throws Exception {
        DeleteRequest request = new DeleteRequest();
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        Observable<byte[]> observable = BulkActionMarshaller.lazyConvertToBytes(Collections.<ActionRequest>singletonList(request));
        assertHasSize(observable, 2);

        byte[] bytes = takeNth(observable, 0);
        String actionAsString = new String(bytes);
        assertThat(actionAsString).isEqualTo("{\"delete\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\"}}");

        bytes = takeNth(observable, 1);
        assertThat(new String(bytes)).isEqualTo("\n");
    }

    @Test
    public void should_marshall_update_request() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        request.doc("foo", "bar");
        Observable<byte[]> observable = BulkActionMarshaller.lazyConvertToBytes(Collections.<ActionRequest>singletonList(request));
        assertHasSize(observable, 4);

        byte[] bytes = takeNth(observable, 0);
        assertThat(new String(bytes)).isEqualTo("{\"update\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"_retry_on_conflict\":0}}");
        bytes = takeNth(observable, 1);
        assertThat(new String(bytes)).isEqualTo("\n");
        bytes = takeNth(observable, 2);
        assertThat(new String(bytes)).isEqualTo("{\"doc\":{\"foo\":\"bar\"}}");
        bytes = takeNth(observable, 3);
        assertThat(new String(bytes)).isEqualTo("\n");
    }

    @Test
    public void should_marshall_update_request_with_upsert() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        request.upsert("bar", "baz");
        Observable<byte[]> observable = BulkActionMarshaller.lazyConvertToBytes(Collections.<ActionRequest>singletonList(request));
        assertHasSize(observable, 4);

        byte[] bytes = takeNth(observable, 0);
        assertThat(new String(bytes)).isEqualTo("{\"update\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"_retry_on_conflict\":0}}");

        bytes = takeNth(observable, 1);
        assertThat(new String(bytes)).isEqualTo("\n");

        bytes = takeNth(observable, 2);
        assertThat(new String(bytes)).isEqualTo("{\"upsert\":{\"bar\":\"baz\"}}");

        bytes = takeNth(observable, 3);
        assertThat(new String(bytes)).isEqualTo("\n");
    }

    @Test
    public void should_marshall_update_request_with_doc_as_upsert() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        request.doc("bar", "baz");
        request.docAsUpsert(true);
        Observable<byte[]> observable = BulkActionMarshaller.lazyConvertToBytes(Collections.<ActionRequest>singletonList(request));
        assertHasSize(observable, 4);

        byte[] bytes = takeNth(observable, 0);
        assertThat(new String(bytes)).isEqualTo("{\"update\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"_retry_on_conflict\":0}}");

        bytes = takeNth(observable, 1);
        assertThat(new String(bytes)).isEqualTo("\n");

        bytes = takeNth(observable, 2);
        assertThat(new String(bytes)).isEqualTo("{\"doc_as_upsert\":true,\"doc\":{\"bar\":\"baz\"}}");

        bytes = takeNth(observable, 3);
        assertThat(new String(bytes)).isEqualTo("\n");
    }

    @Test
    public void should_marshall_update_request_with_script() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        request.script("the_script");
        Observable<byte[]> observable = BulkActionMarshaller.lazyConvertToBytes(Collections.<ActionRequest>singletonList(request));
        assertHasSize(observable, 4);

        byte[] bytes = takeNth(observable, 0);
        assertThat(new String(bytes)).isEqualTo("{\"update\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"_retry_on_conflict\":0}}");

        bytes = takeNth(observable, 1);
        assertThat(new String(bytes)).isEqualTo("\n");

        bytes = takeNth(observable, 2);
        assertThat(new String(bytes)).isEqualTo("{\"script\":\"the_script\"}");

        bytes = takeNth(observable, 3);
        assertThat(new String(bytes)).isEqualTo("\n");
    }

    @Test
    public void should_marshall_update_request_with_script_and_upsert() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index("the_index");
        request.type("the_type");
        request.id("the_id");
        request.script("the_script");
        request.upsert("bar", "baz");
        Observable<byte[]> observable = BulkActionMarshaller.lazyConvertToBytes(Collections.<ActionRequest>singletonList(request));
        assertHasSize(observable, 4);

        byte[] bytes = takeNth(observable, 0);
        assertThat(new String(bytes)).isEqualTo("{\"update\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"_retry_on_conflict\":0}}");

        bytes = takeNth(observable, 1);
        assertThat(new String(bytes)).isEqualTo("\n");

        bytes = takeNth(observable, 2);
        assertThat(new String(bytes)).isEqualTo("{\"upsert\":{\"bar\":\"baz\"},\"script\":\"the_script\"}");

        bytes = takeNth(observable, 3);
        assertThat(new String(bytes)).isEqualTo("\n");
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

        Observable<byte[]> observable = BulkActionMarshaller.lazyConvertToBytes(Arrays.<ActionRequest>asList(update, delete, index));
        assertHasSize(observable, 10);

        // update request
        byte[] bytes = takeNth(observable, 0);
        assertThat(new String(bytes)).isEqualTo("{\"update\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"_retry_on_conflict\":0}}");
        bytes = takeNth(observable, 1);
        assertThat(new String(bytes)).isEqualTo("\n");
        bytes = takeNth(observable, 2);
        assertThat(new String(bytes)).isEqualTo("{\"upsert\":{\"bar\":\"baz\"},\"script\":\"the_script\"}");
        bytes = takeNth(observable, 3);
        assertThat(new String(bytes)).isEqualTo("\n");

        // delete request
        bytes = takeNth(observable, 4);
        assertThat(new String(bytes)).isEqualTo("{\"delete\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\"}}");
        bytes = takeNth(observable, 5);
        assertThat(new String(bytes)).isEqualTo("\n");

        // index request
        bytes = takeNth(observable, 6);
        assertThat(new String(bytes)).isEqualTo("{\"index\":{\"_index\":\"the_index\",\"_type\":\"the_type\",\"_id\":\"the_id\",\"replication\":\"default\",\"op_type\":\"index\"}}");
        bytes = takeNth(observable, 7);
        assertThat(new String(bytes)).isEqualTo("\n");
        bytes = takeNth(observable, 8);
        assertThat(bytes).isNotNull();
        assertThat(new String(bytes)).isEqualTo("{\"foo\":\"bar\"}");
        bytes = takeNth(observable, 9);
        assertThat(new String(bytes)).isEqualTo("\n");
    }

}