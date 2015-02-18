package com.github.obourgain.elasticsearch.http.handler.document.index;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class IndexResponse {

    private String index;
    private String type;
    private String id;
    private long version;
    private boolean created;

}
