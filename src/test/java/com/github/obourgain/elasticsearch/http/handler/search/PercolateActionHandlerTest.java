package com.github.obourgain.elasticsearch.http.handler.search;

import static com.github.obourgain.elasticsearch.http.TestFilesUtils.readFromClasspath;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.percolate.PercolateResponse;

public class PercolateActionHandlerTest extends AbstractTest {

    @Test
    public void should_percolate() throws IOException, ExecutionException, InterruptedException {
        createMapping();

        XContentBuilder query = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("query")
                        .startObject("match")
                            .field("message", "bonsai tree")
                        .endObject()
                    .endObject()
                .endObject();

        String string = query.string();

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

    private void createMapping() {
        String mappingSource = readFromClasspath("com/github/obourgain/elasticsearch/http/handler/search/percolate-mapping.json");
        transportClient.admin().indices().putMapping(Requests.putMappingRequest(THE_INDEX).type("my-type").source(mappingSource)).actionGet();

        ensureSearchable(THE_INDEX);
        refresh();
    }
}