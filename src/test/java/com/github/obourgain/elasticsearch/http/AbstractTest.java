package com.github.obourgain.elasticsearch.http;

import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.elasticsearch.node.internal.InternalNode.HTTP_ENABLED;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.MapEntry;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.github.obourgain.elasticsearch.http.client.HttpClient;
import com.github.obourgain.elasticsearch.http.response.entity.Shards;

/**
 * @author olivier bourgain
 */
@ThreadLeakFilters(defaultFilters = true, filters = {RxNettyThreadFilter.class})
@ElasticsearchIntegrationTest.ClusterScope(transportClientRatio = 1, numClientNodes = 1, numDataNodes = 1, scope = ElasticsearchIntegrationTest.Scope.SUITE)
public abstract class AbstractTest extends ElasticsearchIntegrationTest {

    public static final String THE_INDEX = "the_index";
    public static final String THE_TYPE = "the_type";
    public static final String THE_ID = "the_id";

    protected TransportClient transportClient;
    protected HttpClient httpClient;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(HTTP_ENABLED, true)
                .build();
    }

    @Before
    public void setUpClient() throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        createIndex(THE_INDEX);
        ensureSearchable(THE_INDEX);

        transportClient = (TransportClient) cluster().client();
        String url = getUrlOfOneNode();
        httpClient = new HttpClient(Collections.singleton(url));
    }

    protected String getUrlOfOneNode() {
        NodeInfo[] nodes = admin().cluster().nodesInfo(Requests.nodesInfoRequest()).actionGet().getNodes();
        Assert.assertThat(nodes.length, Matchers.greaterThanOrEqualTo(1));

        TransportAddress transportAddress = nodes[0].getHttp().getAddress().publishAddress();
        Assert.assertEquals(InetSocketTransportAddress.class, transportAddress.getClass());
        InetSocketTransportAddress inetSocketTransportAddress = (InetSocketTransportAddress) transportAddress;
        InetSocketAddress socketAddress = inetSocketTransportAddress.address();

        return String.format("http://%s:%d", socketAddress.getHostName(), socketAddress.getPort());
    }

    @After
    public void stopClient() {
        httpClient.close();
    }


    private XContentType initialContentType = null;

    @Before
    public void save_request_content_type() {
        // this client only uses JSON, so set the content type used by ES on request generation, but also clean up stuff afterwards
        initialContentType = Requests.CONTENT_TYPE;
        Requests.CONTENT_TYPE = JSON;
    }

    @After
    public void restore_request_content_type() {
        Requests.CONTENT_TYPE = initialContentType;
    }

    protected void createDoc() throws IOException {
        createSimpleDoc(THE_INDEX, THE_TYPE, THE_ID);
    }

    protected void createSimpleDoc(String index, String type, String id) throws IOException {
        transportClient.index(Requests.indexRequest().index(index).type(type).id(id).refresh(true)
                        .source(XContentFactory.jsonBuilder().startObject()
                                .field("the_string_field", "the_string_value")
                                .field("the_integer_field", 42)
                                .field("the_boolean_field", true)
                                .field("the_long_array_field", new Long[]{42L, 53L})
                                .endObject())
        ).actionGet();
    }

    protected void createDocFromClasspathFile(String index, String type, String id, String path) throws IOException {
        String document = TestFilesUtils.readFromClasspath(path);
        createDoc(index, type, id, document);
    }

    protected void createDoc(String index, String type, String id, String document) throws IOException {
        transportClient.index(Requests.indexRequest().index(index).type(type).id(id).refresh(true).source(document)).actionGet();
    }

    protected void deleteDoc(String index, String type, String id) {
        transportClient.delete(Requests.deleteRequest(index).type(type).id(id).refresh(true)).actionGet();
    }

    protected void createPercolatorQuery() throws IOException {
        QueryBuilder qb = QueryBuilders.matchQuery("the_string_field", "the_string_value");

        transportClient.prepareIndex(THE_INDEX, ".percolator", "the_query")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("query", qb)
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();
    }

    protected void compareMap(Map<String, Object> expected, Map<String, Object> actualSource) {
        Assertions.assertThat(actualSource).hasSameSizeAs(expected);
        for (Map.Entry<String, Object> actualEntry : actualSource.entrySet()) {
            Assertions.assertThat(expected).contains(MapEntry.entry(actualEntry.getKey(), actualEntry.getValue()));
        }
    }

    protected XContentBuilder source() throws IOException {
        return XContentFactory.jsonBuilder().startObject()
                .field("the_string_field", "the_string_value")
                .field("the_integer_field", 42)
                .field("the_boolean_field", true)
                .field("the_long_array_field", new Long[]{42L, 53L})
                .endObject();
    }

    public void assertShardsSuccessfulForIT(Shards shards, String indexName) {
        Assertions.assertThat(shards.getTotal()).describedAs(shards.toString()).isEqualTo(getNumShards(indexName).numPrimaries);
        Assertions.assertThat(shards.getSuccessful()).describedAs(shards.toString()).isEqualTo(getNumShards(indexName).numPrimaries);
        Assertions.assertThat(shards.getFailed()).describedAs(shards.toString()).isEqualTo(0);
    }

    public void assertSettingsEquals(Settings expected, Settings actual) {
        for (Map.Entry<String, String> entry : expected.getAsMap().entrySet()) {
            Assertions.assertThat(actual.get(entry.getKey())).isEqualTo(entry.getValue());
        }
    }

    public void assertMappingsEquals(Map<String, Object> expected, MappingMetaData actual) {
        Map<String, Object> sourceAsMap;
        try {
            sourceAsMap = actual.getSourceAsMap();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (Map.Entry<String, Object> entry : expected.entrySet()) {
            String key = entry.getKey();
            Assertions.assertThat(sourceAsMap.containsKey(key)).isTrue();
            Object actualValue = sourceAsMap.get(key);
            if (entry.getValue() instanceof Map) {
                assertMapContainsValues((Map) entry.getValue(), ((Map) sourceAsMap.get(entry.getKey())));
            } else {
                Assertions.assertThat(actualValue).isEqualTo(entry.getValue());
            }
        }
    }

    protected void assertMapContainsValues(Map<Object, Object> expectedValues, Map<Object, Object> actual) {
        for (Map.Entry entry : expectedValues.entrySet()) {
            Assertions.assertThat(actual.containsKey(entry.getKey())).isTrue();
            Assertions.assertThat(actual.get(entry.getKey())).isEqualTo(entry.getValue());
        }
    }

    protected Map<String, Object> mappingAsJsonToMap(String mappingAsJson) {
        ObjectMapper objectMapper = new ObjectMapper();
        MapType mapType = objectMapper.getTypeFactory().constructMapType(HashMap.class, String.class, Object.class);
        try {
            return new ObjectMapper().readValue(mappingAsJson, mapType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
