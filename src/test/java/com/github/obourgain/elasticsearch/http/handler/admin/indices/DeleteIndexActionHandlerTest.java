package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.HttpClient;
import com.github.obourgain.elasticsearch.http.response.admin.indices.delete.DeleteIndexResponse;

@ElasticsearchIntegrationTest.ClusterScope(transportClientRatio = 1, numClientNodes = 1, numDataNodes = 1, scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class DeleteIndexActionHandlerTest extends ElasticsearchIntegrationTest {

    private HttpClient httpClient;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("action.destructive_requires_name", false)
                .put(super.nodeSettings(nodeOrdinal)).build();
    }

    @Before
    public void setUpClient() throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        NodeInfo[] nodes = admin().cluster().nodesInfo(Requests.nodesInfoRequest()).actionGet().getNodes();
        Assert.assertThat(nodes.length, Matchers.greaterThanOrEqualTo(1));

        TransportAddress transportAddress = nodes[0].getHttp().getAddress().publishAddress();
        Assert.assertEquals(InetSocketTransportAddress.class, transportAddress.getClass());
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;
        InetSocketAddress socketAddress = inetSocketTransportAddress.address();

        String url = String.format("http://%s:%d", socketAddress.getHostName(), socketAddress.getPort());
        httpClient = new HttpClient(Collections.singleton(url));
    }

    @Test
    public void should_delete_index() throws Exception {
        Assertions.assertThat(indexExists("the_index")).isTrue();

        DeleteIndexResponse response = httpClient.admin().indices().deleteIndex(Requests.deleteIndexRequest("the_index")).get();

        Assertions.assertThat(response.isAcknowledged()).isTrue();
        Assertions.assertThat(response.getError()).isNull();
        Assertions.assertThat(response.getStatus()).isEqualTo(200);
        Assertions.assertThat(indexExists("the_index")).isFalse();
    }

    @Test
    public void should_delete_all_indices_for_wildcard() throws Exception {
        createIndex("test1");
        createIndex("test2");

        DeleteIndexResponse response = httpClient.admin().indices().deleteIndex(Requests.deleteIndexRequest("_all")).get();

        Assertions.assertThat(response.isAcknowledged()).isTrue();
        Assertions.assertThat(response.getError()).isNull();
        Assertions.assertThat(response.getStatus()).isEqualTo(200);
        Assertions.assertThat(indexExists("test1")).isFalse();
        Assertions.assertThat(indexExists("test2")).isFalse();
    }

    @Test
    public void should_fail_on_missing_index() throws Exception {
        Assertions.assertThat(indexExists("the_index")).isTrue();

        DeleteIndexResponse response = httpClient.admin().indices().deleteIndex(Requests.deleteIndexRequest("doesnotexists")).get();

        Assertions.assertThat(response.isAcknowledged()).isFalse();
        Assertions.assertThat(response.getError()).contains("IndexMissingException");
        Assertions.assertThat(response.getError()).contains("doesnotexists");
        Assertions.assertThat(response.getStatus()).isEqualTo(404);
    }
}