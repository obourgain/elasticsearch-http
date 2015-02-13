package com.github.obourgain.elasticsearch.http.handler.document.deleteByQuery;

import com.github.obourgain.elasticsearch.http.response.entity.Indices;
import lombok.Getter;
import lombok.Builder;

@Builder
@Getter
public class DeleteByQueryResponse {

    private Indices indices;

}
