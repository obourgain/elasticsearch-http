package com.github.obourgain.elasticsearch.http.handler.admin.indices.validate;

import org.assertj.core.api.Assertions;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.AbstractTest;

public class ValidateQueryActionHandlerTest extends AbstractTest {

    @Test
    public void should_validate() throws Exception {
        ValidateQueryResponse response = httpClient.admin().indices().validateQuery(new ValidateQueryRequest(THE_INDEX).source(new QuerySourceBuilder().setQuery(QueryBuilders.matchAllQuery()))).get();

        Assertions.assertThat(response.isValid()).isTrue();
    }

    @Test
    public void should_not_validate() throws Exception {
        ensureSearchable(THE_INDEX);
        ValidateQueryResponse response = httpClient.admin().indices().validateQuery(new ValidateQueryRequest(THE_INDEX).source("foo")).get();

        Assertions.assertThat(response.isValid()).isFalse();
    }
}