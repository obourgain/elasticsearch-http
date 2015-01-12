package com.github.obourgain.elasticsearch.http.response.deleteByQuery;

import com.github.obourgain.elasticsearch.http.response.entity.Indices;
import lombok.Getter;
import lombok.experimental.Builder;

@Builder
@Getter
public class DeleteByQueryResponse {

    private Indices indices;

}
