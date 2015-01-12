package com.github.obourgain.elasticsearch.http.response.explain;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.xcontent.XContentParser;
import com.ning.http.client.Response;

public class ExplainResponse {

    private String index;
    private String type;
    private String id;
    private boolean matched;
    private Explanation explanation;

    public static ExplainResponse parse(Response response) {
        return null;
    }
}
