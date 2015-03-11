package com.github.obourgain.elasticsearch.http.response.entity;

import org.elasticsearch.common.xcontent.XContentParser;

public interface Converter<T> {

    T convert(XContentParser parser);

}