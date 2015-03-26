package com.github.obourgain.elasticsearch.http.handler.document.multiget;

import lombok.Getter;

@Getter
public class MultiGetResponseError {

    private String index;
    private String type;
    private String id;
    private String error;

    public MultiGetResponseError(String index, String type, String id, String error) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.error = error;
    }
}
