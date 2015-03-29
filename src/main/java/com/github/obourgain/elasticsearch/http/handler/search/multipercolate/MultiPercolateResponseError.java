package com.github.obourgain.elasticsearch.http.handler.search.multipercolate;

import lombok.Getter;

@Getter
public class MultiPercolateResponseError {

    private final String error;

    public MultiPercolateResponseError(String error) {
        this.error = error;
    }

    @Override
    public String toString() {
        return "MultiPercolateResponseError{" +
                "error='" + error + '\'' +
                '}';
    }
}
