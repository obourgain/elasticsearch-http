package com.github.obourgain.elasticsearch.http.handler.document.update;

import lombok.Getter;
import lombok.experimental.Builder;

@Builder
@Getter
public class UpdateResponse {

    private String index;
    private String type;
    private String id;
    private long version;
    private boolean created;

}
