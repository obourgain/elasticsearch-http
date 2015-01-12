package com.github.obourgain.elasticsearch.http.handler;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * @author olivier bourgain
 */
public interface ActionHandler<Req extends ActionRequest, Resp extends ActionResponse, ReqBuilder extends ActionRequestBuilder<Req, Resp, ReqBuilder, ? extends ElasticsearchClient>> {

    GenericAction<Req, Resp> getAction();

    void execute(Req request, ActionListener<Resp> listener);

}
