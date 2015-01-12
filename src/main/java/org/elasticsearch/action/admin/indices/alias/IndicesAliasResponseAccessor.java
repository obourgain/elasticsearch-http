package org.elasticsearch.action.admin.indices.alias;

/**
 * @author olivier bourgain
 */
public class IndicesAliasResponseAccessor {

    public static IndicesAliasesResponse create(boolean acknoledge) {
        return new IndicesAliasesResponse(acknoledge);
    }

}
