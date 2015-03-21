package com.github.obourgain.elasticsearch.http.handler.search;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.handler.search.percolate.PercolateResponse;
import com.github.obourgain.elasticsearch.http.response.entity.Highlight;
import com.github.obourgain.elasticsearch.http.response.entity.aggs.Terms;

public class PercolateActionHandlerTest extends AbstractTest {

    @Test
    public void should_percolate() throws IOException, ExecutionException, InterruptedException {
        createMapping();

        XContentBuilder query = XContentFactory.jsonBuilder()
                .startObject()
                .field("query", matchQuery("message", "bonsai tree"))
                .endObject();

        transportClient.index(Requests.indexRequest(THE_INDEX).type(".percolator").source(query)).actionGet();

        refresh();

        PercolateSourceBuilder.DocBuilder doc = new PercolateSourceBuilder.DocBuilder()
                .setDoc(XContentFactory.jsonBuilder().startObject().field("message", "A new bonsai tree in the office").endObject());
        PercolateRequest request = new PercolateRequest().indices(THE_INDEX).documentType("my-type")
                .source(new PercolateSourceBuilder().setTrackScores(true).setDoc(doc));

        long start = System.currentTimeMillis();
        PercolateResponse response = httpClient.percolate(request).get();
        long end = System.currentTimeMillis();

        assertShardsSuccessfulForIT(response.getShards(), THE_INDEX);
        Assertions.assertThat(response.getTookInMillis()).isLessThan(end - start);

        Assertions.assertThat(response.getTotal()).isEqualTo(1);
        Assertions.assertThat(response.getMatches()).hasSize(1);
    }

    @Test
    public void should_percolate_with_highlight() throws IOException, ExecutionException, InterruptedException {
        createMapping();

        Requests.CONTENT_TYPE = XContentType.JSON;

        XContentBuilder query = XContentFactory.jsonBuilder()
                .startObject()
                .field("query", matchQuery("message", "bonsai tree"))
                .endObject();
        transportClient.index(Requests.indexRequest(THE_INDEX).type(".percolator").source(query)).actionGet();

        refresh();

        PercolateSourceBuilder.DocBuilder doc = new PercolateSourceBuilder.DocBuilder()
                .setDoc(XContentFactory.jsonBuilder().startObject().field("message", "A new bonsai tree in the office").endObject());
        PercolateRequest request = new PercolateRequest().indices(THE_INDEX).documentType("my-type")
                .source(new PercolateSourceBuilder().setTrackScores(true).setDoc(doc)
                        .setHighlightBuilder(new HighlightBuilder()
                                .field("message").preTags("<foo>").postTags("<bar>")).setSize(5));

        PercolateResponse response = httpClient.percolate(request).get();

        Assertions.assertThat(response.getTotal()).isEqualTo(1);
        Assertions.assertThat(response.getMatches()).hasSize(1);

        Assertions.assertThat(response.getMatches()).hasSize(1);
        Map<String, Highlight> highlight = response.getMatches().getMatches().get(0).getHighlights();
        Assertions.assertThat(highlight).isNotNull().hasSize(1);
        Assertions.assertThat(highlight.get("message").getValue()).contains("<foo>").contains("<bar>").contains("bonsai").contains("tree");
    }

    @Test
    public void should_percolate_with_agg() throws IOException, ExecutionException, InterruptedException {
        // some parts of this code taken from Elasticsearch's test suite
        createMapping();

        client().prepareIndex(THE_INDEX, PercolatorService.TYPE_NAME, Integer.toString(1))
                .setSource(jsonBuilder().startObject()
                        .field("query", matchQuery("field1", 1))
                        .field("some_metadata", "b")
                        .endObject())
                .execute().actionGet();

        client().admin().indices().prepareRefresh(THE_INDEX).execute().actionGet();

        PercolateRequestBuilder percolateRequestBuilder = client().preparePercolate()
                .setIndices(THE_INDEX).setDocumentType("my-type")
                .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", 1).endObject()));

        Aggregator.SubAggCollectionMode aggCollectionMode = randomFrom(Aggregator.SubAggCollectionMode.values());
        percolateRequestBuilder.addAggregation(AggregationBuilders.terms("the_terms").field("some_metadata").collectMode(aggCollectionMode));

        refresh();

        PercolateResponse response = httpClient.percolate(percolateRequestBuilder.request()).get();

        Assertions.assertThat(response.getTotal()).isEqualTo(1);
        Assertions.assertThat(response.getMatches()).hasSize(1);

        Terms terms = response.getAggregations().getTerms("the_terms");
        Assertions.assertThat(terms.getBuckets()).hasSize(1);
        Assertions.assertThat(terms.getBuckets().get(0).getKey()).isEqualTo("b");
    }

    private void createMapping() {
        String mappingSource = readFromClasspath("com/github/obourgain/elasticsearch/http/handler/search/percolate-mapping.json");
        transportClient.admin().indices().putMapping(Requests.putMappingRequest(THE_INDEX).type("my-type").source(mappingSource)).actionGet();

        ensureSearchable(THE_INDEX);
        refresh();
    }
}