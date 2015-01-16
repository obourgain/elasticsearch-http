package com.github.obourgain.elasticsearch.http.handler.document;

import static org.elasticsearch.client.Requests.indexAliasesRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.moreLikeThisRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.lookup.SourceLookup;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.entity.Hit;
import com.github.obourgain.elasticsearch.http.response.search.SearchResponse;

public class MoreLikeThisActionHandlerTest extends AbstractTest {

    @Test
    public void should_() throws Exception {
        BytesReference source = source().bytes();
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);
        index(THE_INDEX, THE_TYPE, THE_ID, expected);

        refresh();

        MoreLikeThisRequest request = new MoreLikeThisRequest(THE_INDEX).type(THE_TYPE).id(THE_ID).searchSource(new SearchSourceBuilder().query(matchAllQuery()));
        long start = System.currentTimeMillis();
        SearchResponse searchResponse = httpClient.moreLikeThis(request).get();
        long end = System.currentTimeMillis();

        Assertions.assertThat(searchResponse.getTookInMillis()).isLessThan(end - start);
        Assertions.assertThat(searchResponse.getScrollId()).isNull();

        assertShardsSuccessfulForIT(searchResponse.getShards(), THE_INDEX);

        // TODO generate a dataset and match those docs
    }

    // TODO maintain these tests, taken from org.elasticsearch.mlt.MoreLikeThisActionTests

    @Test
    public void testSimpleMoreLikeThis() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "string").endObject()
                        .endObject().endObject().endObject()));

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client().index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("text", "lucene").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("2").source(jsonBuilder().startObject().field("text", "lucene release").endObject())).actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running moreLikeThis");
        SearchResponse mltResponse = httpClient.moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1)).get();
        assertHitCount(mltResponse, 1l);
    }

    @Test
    public void testSimpleMoreLikeOnLongField() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1", "some_long", "type=long"));
        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client().index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("some_long", 1367484649580l).endObject())).actionGet();
        client().index(indexRequest("test").type("type2").id("2").source(jsonBuilder().startObject().field("some_long", 0).endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("3").source(jsonBuilder().startObject().field("some_long", -666).endObject())).actionGet();


        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running moreLikeThis");
        SearchResponse mltResponse = httpClient.moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1)).get();
        assertHitCount(mltResponse, 0l);
    }

    @Test
    public void testMoreLikeThisWithAliases() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "string").endObject()
                        .endObject().endObject().endObject()));
        logger.info("Creating aliases alias release");
        client().admin().indices().aliases(indexAliasesRequest().addAlias("release", termFilter("text", "release"), "test")).actionGet();
        client().admin().indices().aliases(indexAliasesRequest().addAlias("beta", termFilter("text", "beta"), "test")).actionGet();

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client().index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("text", "lucene beta").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("2").source(jsonBuilder().startObject().field("text", "lucene release").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("3").source(jsonBuilder().startObject().field("text", "elasticsearch beta").endObject())).actionGet();
        client().index(indexRequest("test").type("type1").id("4").source(jsonBuilder().startObject().field("text", "elasticsearch release").endObject())).actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running moreLikeThis on index");
        SearchResponse mltResponse = httpClient.moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1)).get();
        assertHitCount(mltResponse, 2l);

        logger.info("Running moreLikeThis on beta shard");
        mltResponse = httpClient.moreLikeThis(moreLikeThisRequest("beta").type("type1").id("1").minTermFreq(1).minDocFreq(1)).get();
        assertHitCount(mltResponse, 1l);
        assertThat(mltResponse.getHits().getAt(0).getId(), equalTo("3"));

        logger.info("Running moreLikeThis on release shard");
        mltResponse = httpClient.moreLikeThis(moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1).searchIndices("release")).get();
        assertHitCount(mltResponse, 1l);
        assertThat(mltResponse.getHits().getAt(0).getId(), equalTo("2"));
        // remove node client from copied test
    }

    // no need to copy
    // testMoreLikeThisIssue2197
    // testMoreLikeWithCustomRouting
    // testMoreLikeThisIssueRoutingNotSerialized
    // testNumericField


    @Test
    public void testSimpleMoreLikeInclude() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "string").endObject()
                        .endObject().endObject().endObject()));

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        client().index(indexRequest("test").type("type1").id("1").source(
                jsonBuilder().startObject()
                        .field("text", "Apache Lucene is a free/open source information retrieval software library").endObject()))
                .actionGet();
        client().index(indexRequest("test").type("type1").id("2").source(
                jsonBuilder().startObject()
                        .field("text", "Lucene has been ported to other programming languages").endObject()))
                .actionGet();
        client().admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Running More Like This with include true");
        SearchResponse mltResponse = httpClient.moreLikeThis(
                moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1).include(true).percentTermsToMatch(0))
                .get();
        assertOrderedSearchHits(mltResponse, "1", "2");

        mltResponse = httpClient.moreLikeThis(
                moreLikeThisRequest("test").type("type1").id("2").minTermFreq(1).minDocFreq(1).include(true).percentTermsToMatch(0))
                .get();
        assertOrderedSearchHits(mltResponse, "2", "1");

        logger.info("Running More Like This with include false");
        mltResponse = httpClient.moreLikeThis(
                moreLikeThisRequest("test").type("type1").id("1").minTermFreq(1).minDocFreq(1).percentTermsToMatch(0))
                .get();
        assertSearchHits(mltResponse, "2");
    }

    @Test
    public void testMoreLikeThisBodyFromSize() throws Exception {
        String index = "test";
        logger.info("Creating index test");
        assertAcked(prepareCreate(index).addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "string").endObject()
                        .endObject().endObject().endObject()));

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        List<IndexRequestBuilder> builders = new ArrayList<>(10);
        for (int i = 1; i <= 10; i++) {
            builders.add(client().prepareIndex(index, "type1").setSource("text", "lucene").setId(String.valueOf(i)));
        }
        indexRandom(true, builders);

        logger.info("'size' set but 'search_from' and 'search_size' kept to defaults");
        SearchResponse mltResponse = httpClient.moreLikeThis(
                moreLikeThisRequest(index).type("type1").id("1").minTermFreq(1).minDocFreq(1).include(true)
                        .searchSource(SearchSourceBuilder.searchSource().size(5)))
                .get();
        assertShardsSuccessfulForIT(mltResponse.getShards(), "test");
        assertEquals(mltResponse.getHits().getHits().size(), 5);

        logger.info("'from' set but 'search_from' and 'search_size' kept to defaults");
        mltResponse = httpClient.moreLikeThis(
                moreLikeThisRequest(index).type("type1").id("1").minTermFreq(1).minDocFreq(1).include(true)
                        .searchSource(SearchSourceBuilder.searchSource().from(5)))
                .get();
        assertShardsSuccessfulForIT(mltResponse.getShards(), "test");
        assertEquals(mltResponse.getHits().getHits().size(), 5);

        logger.info("When set, 'search_from' and 'search_size' should override 'from' and 'size'");
        mltResponse = httpClient.moreLikeThis(
                moreLikeThisRequest(index).type("type1").id("1").minTermFreq(1).minDocFreq(1).include(true)
                        .searchSize(10).searchFrom(2)
                        .searchSource(SearchSourceBuilder.searchSource().size(1).from(1)))
                .get();
        assertShardsSuccessfulForIT(mltResponse.getShards(), index);
        assertEquals(mltResponse.getHits().getHits().size(), 8);
    }

    @Test
    public void testSimpleMoreLikeThisIds() throws Exception {
        logger.info("Creating index test");
        assertAcked(prepareCreate("test").addMapping("type1",
                jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("text").field("type", "string").endObject()
                        .endObject().endObject().endObject()));

        logger.info("Running Cluster Health");
        assertThat(ensureGreen(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Indexing...");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("test", "type1").setSource("text", "lucene").setId("1"));
        builders.add(client().prepareIndex("test", "type1").setSource("text", "lucene release").setId("2"));
        builders.add(client().prepareIndex("test", "type1").setSource("text", "apache lucene").setId("3"));
        indexRandom(true, builders);

        logger.info("Running MoreLikeThis");
        MoreLikeThisQueryBuilder queryBuilder = QueryBuilders.moreLikeThisQuery("text").ids("1").include(true).minTermFreq(1).minDocFreq(1);
        SearchResponse mltResponse = httpClient.search(Requests.searchRequest("test").types("type1").source(new SearchSourceBuilder().query(queryBuilder))).get();
        assertHitCount(mltResponse, 3l);
    }

    private static void assertHitCount(SearchResponse mltResponse, long expectedHitCount) {
        Assertions.assertThat(mltResponse.getHits().getTotal()).isEqualTo(expectedHitCount);
    }

    public static void assertSearchHits(SearchResponse searchResponse, String... ids) {
        Assertions.assertThat(searchResponse.getHits().getHits().size()).isEqualTo(ids.length);

        Set<String> idsSet = new HashSet<>(Arrays.asList(ids));
        Assertions.assertThat(searchResponse.getHits().getHits()).extracting("id").containsExactly(idsSet.toArray());
    }

    public static void assertOrderedSearchHits(SearchResponse searchResponse, String... ids) {
        Assertions.assertThat(searchResponse.getHits().getHits().size()).isEqualTo(ids.length);

        for (int i = 0; i < ids.length; i++) {
            Hit hit = searchResponse.getHits().getHits().get(i);
            Assertions.assertThat(hit.getId()).isEqualTo(ids[i]);
        }
    }

}