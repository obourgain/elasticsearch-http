package com.github.obourgain.elasticsearch.http.response;

import java.util.Collections;
import java.util.Map;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class AggregationMetaInfos {

    private final String name;
    private final String type;
    private final XContentBuilder aggConfig;
    private final Map<String, AggregationMetaInfos> children;

    public AggregationMetaInfos(String name, String type, Map<String, AggregationMetaInfos> children, XContentBuilder aggConfig) {
        this.name = name;
        this.type = type;
        this.aggConfig = aggConfig;
        this.children = children != null ? children : Collections.<String, AggregationMetaInfos>emptyMap();
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Map<String, AggregationMetaInfos> getChildren() {
        return children;
    }

    public XContentBuilder getConfig() {
        return aggConfig;
    }

    @Override
    public String toString() {
        return "AggregationMetaInfos{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", children=" + children +
                '}';
    }
}
