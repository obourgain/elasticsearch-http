package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.common.bytes.BytesReference;

/**
 * @author olivier bourgain
 */
public class DeleteByQueryRequestAccessor {

    public static DeleteByQueryResponse build() {
        return new DeleteByQueryResponse();
    }

    public static BytesReference getSource(DeleteByQueryRequest deleteByQueryRequest) {
        return deleteByQueryRequest.source();
    }

    public static String[] types(DeleteByQueryRequest deleteByQueryRequest) {
        return deleteByQueryRequest.types();
    }

}
