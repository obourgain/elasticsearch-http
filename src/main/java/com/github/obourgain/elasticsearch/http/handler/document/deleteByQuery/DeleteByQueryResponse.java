package com.github.obourgain.elasticsearch.http.handler.document.deleteByQuery;

import com.github.obourgain.elasticsearch.http.response.entity.Indices;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class DeleteByQueryResponse {

    private Indices indices;

}
