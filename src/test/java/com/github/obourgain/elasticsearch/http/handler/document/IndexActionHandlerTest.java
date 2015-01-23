package com.github.obourgain.elasticsearch.http.handler.document;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.lookup.SourceLookup;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.ElasticsearchHttpException;
import com.github.obourgain.elasticsearch.http.response.document.index.IndexResponse;

public class IndexActionHandlerTest extends AbstractTest {

    @Test
    public void should_index_document() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        IndexRequest request = Requests.indexRequest().index(THE_INDEX)
                .type(THE_TYPE)
                .id(THE_ID)
                .source(source.toBytes());
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);

        IndexResponse response = httpClient.index(request).get();
        compare(expected, response);
    }

    @Test
    public void should_not_index_document_when_version_does_not_match() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        IndexRequest request = Requests.indexRequest().index(THE_INDEX)
                .type(THE_TYPE)
                .id(THE_ID)
                .version(1)
                .source(source.toBytes());
        transportClient.index(request);

        try {
            httpClient.index(request).get();
            fail();
        } catch (ExecutionException e) {
            Assertions.assertThat(e).hasCauseInstanceOf(ElasticsearchHttpException.class);
            Assertions.assertThat(e.getCause()).hasMessageStartingWith("status code 409");
            Assertions.assertThat(e.getCause()).hasMessageContaining("VersionConflictEngineException");
        }
    }

    @Test
    public void should_not_index_document_when_already_exists_and_op_create() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        IndexRequest request = Requests.indexRequest().index(THE_INDEX)
                .type(THE_TYPE)
                .id(THE_ID)
                .opType(IndexRequest.OpType.CREATE)
                .source(source.toBytes());
        transportClient.index(request);

        try {
            httpClient.index(request).get();
            fail();
        } catch (ExecutionException e) {
            Assertions.assertThat(e).hasCauseInstanceOf(ElasticsearchHttpException.class);
            Assertions.assertThat(e.getCause()).hasMessageStartingWith("status code 409");
            Assertions.assertThat(e.getCause()).hasMessageContaining("DocumentAlreadyExistsException");
        }
    }

    @Test
    public void should_generate_id_when_not_specified() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        IndexRequest request = Requests.indexRequest().index(THE_INDEX)
                .type(THE_TYPE)
                .source(source.toBytes())
                .refresh(true);
        Map<String, Object> expected = SourceLookup.sourceAsMap(source);

        httpClient.index(request).get();

        MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();
        SearchResponse searchResponse = new SearchRequestBuilder(transportClient).setQuery(query).setIndices(THE_INDEX).execute().actionGet();
        Assertions.assertThat(searchResponse.getHits().getTotalHits()).isEqualTo(1);
        Assertions.assertThat(searchResponse.getHits().getHits()[0].sourceAsMap()).hasSameSizeAs(expected);
        compareMap(expected, searchResponse.getHits().getHits()[0].sourceAsMap());
    }

    protected void compare(Map<String, Object> expected, IndexResponse response) {
        Assertions.assertThat(response.getIndex()).isEqualTo(THE_INDEX);
        Assertions.assertThat(response.getType()).isEqualTo(THE_TYPE);
        Assertions.assertThat(response.getId()).isEqualTo(THE_ID);

        GetResponse getResponse = transportClient.get(Requests.getRequest(THE_INDEX).type(THE_TYPE).id(THE_ID)).actionGet();
        Map<String, Object> actualSource = getResponse.getSource();
        compareMap(expected, actualSource);
    }

}