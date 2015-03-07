package com.github.obourgain.elasticsearch.http.handler.admin.indices.exists;

import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.admin.indices.aliases.IndicesAliasesResponse;

public class IndicesAliasesActionHandlerTest extends AbstractTest{

    @Test
    public void should_create_alias() throws Exception {
        String index = "index_without_alias";
        createIndex(index);
        ensureGreen(index);

        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAlias("the_alias", index);
        IndicesAliasesResponse response = httpClient.admin().indices().aliases(request).get();

        Assertions.assertThat(response.isAcknowledged()).isTrue();

        GetAliasesResponse finalState = transportClient.admin().indices().getAliases(new GetAliasesRequest()).actionGet();
        Assertions.assertThat(finalState.getAliases().get(index)).hasSize(1);
        Assertions.assertThat(finalState.getAliases().get(index).get(0).alias()).isEqualTo("the_alias");
        Assertions.assertThat(finalState.getAliases().get(index).get(0).filter()).isNull();
        Assertions.assertThat(finalState.getAliases().get(index).get(0).getIndexRouting()).isNull();
        Assertions.assertThat(finalState.getAliases().get(index).get(0).getSearchRouting()).isNull();
    }

    @Test
    public void should_delete_alias() throws Exception {
        String index = "index_without_alias";
        createIndex(index);
        ensureGreen(index);

        transportClient.admin().indices().aliases(new IndicesAliasesRequest().addAlias("the_alias", index)).actionGet();
        GetAliasesResponse actualAlias = transportClient.admin().indices().getAliases(new GetAliasesRequest().indices(index)).actionGet();
        Assertions.assertThat(actualAlias.getAliases().containsKey(index)).isTrue();
        Assertions.assertThat(actualAlias.getAliases().get(index)).hasSize(1);
        Assertions.assertThat(actualAlias.getAliases().get(index).get(0).alias()).isEqualTo("the_alias");

        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.removeAlias(index, "the_alias");
        IndicesAliasesResponse response = httpClient.admin().indices().aliases(request).get();

        Assertions.assertThat(response.isAcknowledged()).isTrue();

        GetAliasesResponse finalState = transportClient.admin().indices().getAliases(new GetAliasesRequest()).actionGet();
        Assertions.assertThat(finalState.getAliases()).hasSize(0);
    }

    @Test
    public void should_create_alias_with_filter() throws Exception {
        String index = "index_without_alias";
        createIndex(index);
        ensureGreen(index);

        // add a mapping so I can add a filter, else it will fail due to strict resolution of fields
        createDoc(index, THE_TYPE, THE_ID);
        ensureSearchable(index);

        IndicesAliasesRequest request = new IndicesAliasesRequest();
        TermFilterBuilder filter = termFilter("the_string_field", "some_value");

        request.addAlias("the_alias", filter, index);
        IndicesAliasesResponse response = httpClient.admin().indices().aliases(request).get();

        Assertions.assertThat(response.isAcknowledged()).isTrue();

        GetAliasesResponse finalState = transportClient.admin().indices().getAliases(new GetAliasesRequest()).actionGet();
        Assertions.assertThat(finalState.getAliases().get(index)).hasSize(1);
        Assertions.assertThat(finalState.getAliases().get(index).get(0).alias()).isEqualTo("the_alias");

        String requestFilter = request.getAliasActions().get(0).aliasAction().filter();
        CompressedString actualFilter = finalState.getAliases().get(index).get(0).filter();
        Assertions.assertThat(actualFilter.string()).isEqualTo(requestFilter);
    }

    @Test
    public void should_create_alias_with_routing() throws Exception {
        String index = "index_without_alias";
        createIndex(index);
        ensureGreen(index);

        IndicesAliasesRequest request = new IndicesAliasesRequest();

        request.addAliasAction(new AliasAction(AliasAction.Type.ADD).alias("the_alias").index(index).searchRouting("the_search_routing").indexRouting("the_index_routing"));
        IndicesAliasesResponse response = httpClient.admin().indices().aliases(request).get();

        Assertions.assertThat(response.isAcknowledged()).isTrue();

        GetAliasesResponse finalState = transportClient.admin().indices().getAliases(new GetAliasesRequest()).actionGet();
        Assertions.assertThat(finalState.getAliases().get(index)).hasSize(1);
        Assertions.assertThat(finalState.getAliases().get(index).get(0).alias()).isEqualTo("the_alias");
        Assertions.assertThat(finalState.getAliases().get(index).get(0).searchRouting()).isEqualTo("the_search_routing");
        Assertions.assertThat(finalState.getAliases().get(index).get(0).indexRouting()).isEqualTo("the_index_routing");
    }

    @Test
    public void should_perform_all_actions() throws Exception {
        String index = "index_without_alias";
        createIndex(index);
        ensureGreen(index);

        {
            IndicesAliasesRequest request = new IndicesAliasesRequest();
            request.addAliasAction(new AliasAction(AliasAction.Type.ADD).alias("the_alias").index(index));
            IndicesAliasesResponse response = httpClient.admin().indices().aliases(request).get();
            Assertions.assertThat(response.isAcknowledged()).isTrue();
        }

        IndicesAliasesRequest request = new IndicesAliasesRequest();
        request.addAliasAction(new AliasAction(AliasAction.Type.ADD).alias("the_other_alias").index(index).searchRouting("the_search_routing").indexRouting("the_index_routing"));
        request.addAliasAction(new AliasAction(AliasAction.Type.REMOVE).alias("the_alias").index(index));
        IndicesAliasesResponse response = httpClient.admin().indices().aliases(request).get();

        Assertions.assertThat(response.isAcknowledged()).isTrue();

        GetAliasesResponse finalState = transportClient.admin().indices().getAliases(new GetAliasesRequest()).actionGet();
        Assertions.assertThat(finalState.getAliases().get(index)).hasSize(1);
        Assertions.assertThat(finalState.getAliases().get(index).get(0).alias()).isEqualTo("the_other_alias");
        Assertions.assertThat(finalState.getAliases().get(index).get(0).searchRouting()).isEqualTo("the_search_routing");
        Assertions.assertThat(finalState.getAliases().get(index).get(0).indexRouting()).isEqualTo("the_index_routing");
    }

}