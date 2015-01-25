package com.github.obourgain.elasticsearch.http.handler.admin.indices;

import java.util.List;
import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.admin.indices.getaliases.GetAliasesResponse;

public class GetAliasesActionHandlerTest extends AbstractTest {

    @Test
    public void should_get_no_aliases() throws Exception {
        GetAliasesResponse response = httpClient.admin().indices().getAliases(new GetAliasesRequest("_all")).get();

        Assertions.assertThat(response.getAliases()).hasSize(0);
    }

    @Test
    public void should_get_aliases() throws Exception {
        transportClient.admin().indices().aliases(new IndicesAliasesRequest().addAlias("test", THE_INDEX)).actionGet();

        GetAliasesResponse response = httpClient.admin().indices().getAliases(new GetAliasesRequest("_all")).get();

        ImmutableOpenMap<String, List<AliasMetaData>> aliases = response.getAliases();
        Assertions.assertThat(aliases).hasSize(1);
        Assertions.assertThat(aliases.containsKey(THE_INDEX)).isTrue();
        List<AliasMetaData> metaDatas = aliases.get(THE_INDEX);
        Assertions.assertThat(metaDatas).hasSize(1);
        Assertions.assertThat(metaDatas.get(0).alias()).isEqualTo("test");
    }
}