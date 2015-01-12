package com.github.obourgain.elasticsearch.http.response.entity;

import java.util.ArrayList;
import org.elasticsearch.common.xcontent.XContentParser;
import lombok.Getter;
import lombok.experimental.Builder;

@Builder
@Getter
public class Explanation {
    private float value;
    private String description;
    private ArrayList<Explanation> details;

    public static Explanation parse(XContentParser parser) {
        return null;
    }
}
