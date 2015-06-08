package com.github.obourgain.elasticsearch.http.handler.search;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.is;
import java.io.IOException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.exists.ExistsResponse;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

public class Exists extends ElasticsearchIntegrationTest {

    public static final String THE_INDEX = "the_index";
    public static final String THE_TYPE = "the_type";
    public static final String THE_ID = "the_id";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(InternalNode.HTTP_ENABLED, true)
                .build();
    }

    @Test
    public void test() throws IOException {
        for (int i = 0; i < 10; i++) {
            createIndex(THE_INDEX);
            ensureSearchable(THE_INDEX);
            index(THE_INDEX, THE_TYPE, THE_ID, source());
            refresh();

            ExistsRequest existsRequest = new ExistsRequest(THE_INDEX).types(THE_TYPE).source(new QuerySourceBuilder().setQuery(matchAllQuery()));
            ExistsResponse transportResponse = client().exists(existsRequest).actionGet();
            assertThat(transportResponse.exists(), is(true));

            HttpPost post = new HttpPost("http://localhost:9501/_search/exists");
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                CloseableHttpResponse response = httpClient.execute(post);
                String responseContent = EntityUtils.toString(response.getEntity());
                assertThat(responseContent.contains("true"), Matchers.is(true));
            }

            client().admin().indices().delete(new DeleteIndexRequest(THE_INDEX)).actionGet();
        }
    }

    protected XContentBuilder source() throws IOException {
        return XContentFactory.jsonBuilder().startObject()
                .field("just_a_field", "the_value")
                .endObject();
    }

}
