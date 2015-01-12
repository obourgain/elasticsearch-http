package com.github.obourgain.elasticsearch.http.response.get;

import java.util.Map;
import org.elasticsearch.index.get.GetField;
import lombok.Getter;
import lombok.experimental.Builder;

@Builder
@Getter
public class GetResponse {

    private String index;
    private String type;
    private String id;
    private long version;
    private boolean found;
    private Map<String, Object> source;
    private Map<String, GetField> fields;

}
