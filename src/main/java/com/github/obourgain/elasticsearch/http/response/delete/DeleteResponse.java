package com.github.obourgain.elasticsearch.http.response.delete;

import lombok.Getter;
import lombok.experimental.Builder;

@Builder
@Getter
public class DeleteResponse {

    private String index;
    private String type;
    private String id;
    private long version;
    private boolean found;

}
