package com.github.obourgain.elasticsearch.http.handler.search.multipercolate;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.percolate.MultiPercolateRequest;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.handler.search.percolate.PercolateResponse;
import com.github.obourgain.elasticsearch.http.response.entity.Highlight;

public class MultiPercolateActionHandlerTest extends AbstractTest {

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
        MultiPercolateRequest multiPercolateRequest = new MultiPercolateRequest().add(request);
        MultiPercolateResponse response = httpClient.multiPercolate(multiPercolateRequest).get();
        long end = System.currentTimeMillis();

        Assertions.assertThat(response.all()).hasSize(1);
        Assertions.assertThat(response.errors()).hasSize(0);

        PercolateResponse percolateResponse = response.percolated().get(0);

        assertShardsSuccessfulForIT(percolateResponse.getShards(), THE_INDEX);
        Assertions.assertThat(percolateResponse.getTookInMillis()).isLessThan(end - start);

        Assertions.assertThat(percolateResponse.getTotal()).isEqualTo(1);
        Assertions.assertThat(percolateResponse.getMatches()).hasSize(1);
    }

    @Test
    public void should_percolate_existing_doc() throws IOException, ExecutionException, InterruptedException {
        createMapping();

        XContentBuilder query = XContentFactory.jsonBuilder()
                .startObject()
                .field("query", matchQuery("message", "bonsai tree"))
                .endObject();

        transportClient.index(Requests.indexRequest(THE_INDEX).type(".percolator").source(query)).actionGet();

        transportClient.index(new IndexRequest(THE_INDEX, THE_TYPE, THE_ID)
                .source(XContentFactory.jsonBuilder().startObject().field("message", "A new bonsai tree in the office").endObject())).actionGet();

        refresh();

        PercolateRequest request = new PercolateRequest().indices(THE_INDEX).documentType("my-type").getRequest(new GetRequest(THE_INDEX, THE_TYPE, THE_ID));

        MultiPercolateRequest multiPercolateRequest = new MultiPercolateRequest().add(request);
        MultiPercolateResponse response = httpClient.multiPercolate(multiPercolateRequest).get();

        Assertions.assertThat(response.all()).hasSize(1);
        Assertions.assertThat(response.errors()).hasSize(0);

        Assertions.assertThat(response.percolated().get(0).getTotal()).isEqualTo(1);
        Assertions.assertThat(response.percolated().get(0).getMatches()).hasSize(1);
    }

    @Test
    public void should_percolate_several_docs() throws IOException, ExecutionException, InterruptedException {
        createMapping();

        XContentBuilder query = XContentFactory.jsonBuilder()
                .startObject()
                .field("query", matchQuery("message", "bonsai tree"))
                .endObject();
        transportClient.index(Requests.indexRequest(THE_INDEX).type(".percolator").source(query)).actionGet();

        transportClient.index(new IndexRequest(THE_INDEX, THE_TYPE, THE_ID)
                .source(XContentFactory.jsonBuilder().startObject().field("message", "A new bonsai tree in the office").endObject())).actionGet();

        refresh();

        PercolateSourceBuilder.DocBuilder doc1 = new PercolateSourceBuilder.DocBuilder()
                .setDoc(XContentFactory.jsonBuilder().startObject().field("message", "A new bonsai tree in the office").endObject());
        PercolateRequest request1 = new PercolateRequest().indices(THE_INDEX).documentType("my-type")
                .source(new PercolateSourceBuilder().setTrackScores(true).setDoc(doc1));

        PercolateSourceBuilder.DocBuilder doc2 = new PercolateSourceBuilder.DocBuilder()
                .setDoc(XContentFactory.jsonBuilder().startObject().field("message", "A new bonsai tree in the office").endObject());
        PercolateRequest request2 = new PercolateRequest().indices(THE_INDEX).documentType("my-type")
                .source(new PercolateSourceBuilder().setTrackScores(true).setDoc(doc2));

        PercolateRequest request3 = new PercolateRequest().indices(THE_INDEX).documentType("my-type").getRequest(new GetRequest(THE_INDEX, THE_TYPE, THE_ID));

        long start = System.currentTimeMillis();
        MultiPercolateRequest multiPercolateRequest = new MultiPercolateRequest().add(request1).add(request2).add(request3);
        MultiPercolateResponse response = httpClient.multiPercolate(multiPercolateRequest).get();
        long end = System.currentTimeMillis();

        Assertions.assertThat(response.all()).hasSize(3);
        Assertions.assertThat(response.errors()).hasSize(0);

        PercolateResponse percolateResponse1 = response.percolated().get(0);
        assertShardsSuccessfulForIT(percolateResponse1.getShards(), THE_INDEX);
        Assertions.assertThat(percolateResponse1.getTookInMillis()).isLessThan(end - start);
        Assertions.assertThat(percolateResponse1.getTotal()).isEqualTo(1);
        Assertions.assertThat(percolateResponse1.getMatches()).hasSize(1);

        PercolateResponse percolateResponse2 = response.percolated().get(1);
        assertShardsSuccessfulForIT(percolateResponse2.getShards(), THE_INDEX);
        Assertions.assertThat(percolateResponse2.getTotal()).isEqualTo(1);
        Assertions.assertThat(percolateResponse2.getMatches()).hasSize(1);

        PercolateResponse percolateResponse3 = response.percolated().get(2);
        assertShardsSuccessfulForIT(percolateResponse3.getShards(), THE_INDEX);
        Assertions.assertThat(percolateResponse3.getTotal()).isEqualTo(1);
        Assertions.assertThat(percolateResponse3.getMatches()).hasSize(1);
    }

    @Test
    public void should_percolate_with_count_only() throws IOException, ExecutionException, InterruptedException {
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
                .source(new PercolateSourceBuilder().setTrackScores(true).setDoc(doc)).onlyCount(true);

        MultiPercolateRequest multiPercolateRequest = new MultiPercolateRequest().add(request);

        MultiPercolateResponse response = httpClient.multiPercolate(multiPercolateRequest).get();

        Assertions.assertThat(response.all()).hasSize(1);
        Assertions.assertThat(response.errors()).hasSize(0);

        PercolateResponse percolateResponse = response.percolated().get(0);
        assertShardsSuccessfulForIT(percolateResponse.getShards(), THE_INDEX);

        Assertions.assertThat(percolateResponse.getTotal()).isEqualTo(1);
        Assertions.assertThat(percolateResponse.getMatches()).isNull();
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

        MultiPercolateRequest multiPercolateRequest = new MultiPercolateRequest().add(request);

        MultiPercolateResponse response = httpClient.multiPercolate(multiPercolateRequest).get();

        Assertions.assertThat(response.all()).hasSize(1);
        Assertions.assertThat(response.errors()).hasSize(0);

        PercolateResponse percolateResponse = response.percolated().get(0);
        Assertions.assertThat(percolateResponse.getTotal()).isEqualTo(1);
        Assertions.assertThat(percolateResponse.getMatches()).hasSize(1);

        Assertions.assertThat(percolateResponse.getMatches()).hasSize(1);
        Map<String, Highlight> highlight = percolateResponse.getMatches().getMatches().get(0).getHighlights();
        Assertions.assertThat(highlight).isNotNull().hasSize(1);
        Assertions.assertThat(highlight.get("message").getValue()).contains("<foo>").contains("<bar>").contains("bonsai").contains("tree");
    }

    private void createMapping() {
        String mappingSource = readFromClasspath("com/github/obourgain/elasticsearch/http/handler/search/percolate-mapping.json");
        transportClient.admin().indices().putMapping(Requests.putMappingRequest(THE_INDEX).type("my-type").source(mappingSource)).actionGet();

        ensureSearchable(THE_INDEX);
        refresh();
    }

}