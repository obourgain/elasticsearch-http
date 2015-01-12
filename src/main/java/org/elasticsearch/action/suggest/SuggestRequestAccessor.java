package org.elasticsearch.action.suggest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.suggest.Suggest;

/**
 * @author olivier bourgain
 */
public class SuggestRequestAccessor {

    public static BytesReference getSource(SuggestRequest request) {
        return request.suggest();
    }
}
