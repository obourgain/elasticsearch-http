package com.github.obourgain.elasticsearch.http.handler.document;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.HttpClient;

public class BulkActionHandlerTest extends AbstractTest {

    @Before
    public void setUpClient2() throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        String url = String.format("http://%s:%d", "localhost", 9900);
        httpClient = new HttpClient(Collections.singleton(url));
    }

    @Test
    public void should_execute_bulk() throws ExecutionException, InterruptedException {
        BulkResponse response = httpClient.bulk(new BulkRequest()).get();

        Assertions.assertThat(response.hasFailures()).isFalse();
    }

    @Test
    public void should_execute_bulk2() throws ExecutionException, InterruptedException {
        BulkRequest request = new BulkRequest();

        IndexRequest action = new IndexRequest();
        action.source("foo", "bar");
        action.index("the_index");
        action.type("the_type");
        action.id("the_id");

        request.add(action);
        BulkResponse response = httpClient.bulk(request).get();

        Assertions.assertThat(response.hasFailures()).isFalse();
    }

}