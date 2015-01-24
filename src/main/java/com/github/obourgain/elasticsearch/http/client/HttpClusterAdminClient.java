package com.github.obourgain.elasticsearch.http.client;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.support.AbstractClusterAdminClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.ClusterHealthActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.ClusterStateActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.node.hotthreads.NodesHotThreadsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.settings.ClusterUpdateSettingsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.cluster.stats.ClusterStatsActionHandler;
import com.google.common.collect.ImmutableMap;

/**
 * @author olivier bourgain
 */
public class HttpClusterAdminClient extends AbstractClusterAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpClusterAdminClient.class);

    private final HttpClient httpClient;
    private final ImmutableMap<GenericAction, ActionHandler> actionHandlers;

    public HttpClusterAdminClient(HttpClient httpClient) {
        this.httpClient = httpClient;

        ImmutableMap.Builder<GenericAction, ActionHandler> tempActionHandlers = ImmutableMap.builder();
        tempActionHandlers.put(ClusterStateAction.INSTANCE, new ClusterStateActionHandler(this));
        tempActionHandlers.put(ClusterStatsAction.INSTANCE, new ClusterStatsActionHandler(this));
        tempActionHandlers.put(ClusterHealthAction.INSTANCE, new ClusterHealthActionHandler(this));
        tempActionHandlers.put(ClusterUpdateSettingsAction.INSTANCE, new ClusterUpdateSettingsActionHandler(this));
        tempActionHandlers.put(NodesHotThreadsAction.INSTANCE, new NodesHotThreadsActionHandler(this));
        this.actionHandlers = tempActionHandlers.build();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, ClusterAdminClient> action, Request request) {
        PlainActionFuture<Response> future = PlainActionFuture.newFuture();
        ActionRequestValidationException validationException = request.validate();
        if (validationException != null) {
            future.onFailure(validationException);
            return future;
        }
        ActionHandler<Request, Response, RequestBuilder> handler = actionHandlers.get(action);
        if (handler == null) {
            throw new IllegalStateException("no handler found for action " + action);
        }
        handler.execute(request, future);
        return future;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> void execute(Action<Request, Response, RequestBuilder, ClusterAdminClient> action, Request request, ActionListener<Response> listener) {
        ActionHandler<Request, Response, RequestBuilder> handler = actionHandlers.get(action);
        if (handler == null) {
            throw new IllegalStateException("no handler found for action " + action);
        }
        handler.execute(request, listener);
    }

    @Override
    public ThreadPool threadPool() {
        return null;
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }
}
