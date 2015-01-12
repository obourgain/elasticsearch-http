package org.elasticsearch.action.get;

import org.elasticsearch.index.get.GetResult;

/**
 * @author olivier bourgain
 */
public class GetResponseAccessor {

    /**
     * This exposes the package visible constructor {@link GetResponse#GetResponse(GetResult)}.
     */
    public static GetResponse build(GetResult getResult) {
        return new GetResponse(getResult);
    }

}
