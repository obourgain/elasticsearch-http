package org.elasticsearch.action.suggest;

import org.elasticsearch.search.suggest.Suggest;

/**
 * @author olivier bourgain
 */
public class SuggestResponseAccessor {

    public static SuggestResponse build(Suggest suggest, int totalShards, int successfulShards, int failedShards, Object o) {
        return new SuggestResponse(suggest, totalShards, successfulShards, failedShards, null);
    }

}
