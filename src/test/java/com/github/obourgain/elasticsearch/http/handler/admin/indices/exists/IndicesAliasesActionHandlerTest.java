package com.github.obourgain.elasticsearch.http.handler.admin.indices.exists;

import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;
import com.github.obourgain.elasticsearch.http.response.admin.indices.aliases.GetAliasesResponse;

public class IndicesAliasesActionHandlerTest extends AbstractTest{

    @Test
    public void should_get_aliases() throws Exception {
        String index = "index_without_alias";
        createIndex(index);

        GetAliasesResponse response = httpClient.admin().indices().getAliases(new GetAliasesRequest(index)).get();

        Assertions.assertThat(response.getAliases()).hasSize(1);
        Assertions.assertThat(response.getAliases().get(index)).isNotNull();
    }
}