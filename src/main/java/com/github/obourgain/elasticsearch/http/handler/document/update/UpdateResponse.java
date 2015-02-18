package com.github.obourgain.elasticsearch.http.handler.document.update;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class UpdateResponse {

    private String index;
    private String type;
    private String id;
    private long version;
    private boolean created;

}
