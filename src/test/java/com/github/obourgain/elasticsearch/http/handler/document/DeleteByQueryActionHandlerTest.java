package com.github.obourgain.elasticsearch.http.handler.document;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.lookup.SourceLookup;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;

public class DeleteByQueryActionHandlerTest extends AbstractTest {

    public static final String OTHER_INDEX = THE_INDEX + "_2";

    @Test
    public void should_delete_documents() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        index(THE_INDEX, THE_TYPE, THE_ID, SourceLookup.sourceAsMap(source));
        index(THE_INDEX, THE_TYPE, THE_ID + "_2", SourceLookup.sourceAsMap(source));
        refresh();
        Assertions.assertThat(get(THE_INDEX, THE_TYPE, THE_ID).isExists()).isTrue();

        httpClient.deleteByQuery(Requests.deleteByQueryRequest(THE_INDEX)
                        .source(new QuerySourceBuilder().setQuery(QueryBuilders.matchAllQuery()))
        ).get();

        Assertions.assertThat(getFromPrimary(THE_INDEX, THE_TYPE, THE_ID).isExists()).isFalse();
        Assertions.assertThat(getFromPrimary(THE_INDEX, THE_TYPE, THE_ID + "_2").isExists()).isFalse();
    }

    @Test
    public void should_delete_documents_in_multiple_indices() throws IOException, ExecutionException, InterruptedException {
        BytesReference source = source().bytes();
        IndexRequest request = Requests.indexRequest().index(THE_INDEX)
                .type(THE_TYPE)
                .id(THE_ID)
                .source(source.toBytes());
        transportClient.index(request).actionGet();
        request.index(OTHER_INDEX);
        transportClient.index(request).actionGet();
        refresh();

        httpClient.deleteByQuery(Requests.deleteByQueryRequest(THE_INDEX, THE_INDEX + "_2")
                .source(new QuerySourceBuilder().setQuery(QueryBuilders.matchAllQuery()))).get();

        Assertions.assertThat(getFromPrimary(THE_INDEX, THE_TYPE, THE_ID).isExists()).isFalse();
        Assertions.assertThat(getFromPrimary(OTHER_INDEX, THE_TYPE, THE_ID).isExists()).isFalse();
    }

    private GetResponse getFromPrimary(String index, String type, String id) {
        // use primary as per the doc, because delete by query uses quorum as default consistency
        return client().prepareGet(index, type, id).setPreference("_primary").execute().actionGet();
    }


}