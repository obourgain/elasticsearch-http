package com.github.obourgain.elasticsearch.http.client;

import java.util.concurrent.Future;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.CreateIndexActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.DeleteIndexActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.FlushActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.GetAliasesActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.GetMappingsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.GetSettingsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.GetTemplatesActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.IndicesAliasesActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.IndicesExistsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.OptimizeActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.PutIndexTemplateActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.RefreshActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.ValidateQueryActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.close.CloseIndexActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.mapping.put.PutMappingActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.open.OpenIndexActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.settings.UpdateSettingsActionHandler;
import com.github.obourgain.elasticsearch.http.response.admin.indices.close.CloseIndexResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.create.CreateIndexResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.delete.DeleteIndexResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.exists.IndicesExistsResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.flush.FlushResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.getaliases.GetAliasesResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.open.OpenIndexResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.optimize.OptimizeResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.refresh.RefreshResponse;
import com.github.obourgain.elasticsearch.http.response.validate.ValidateQueryResponse;
import com.google.common.collect.ImmutableMap;

/**
 * @author olivier bourgain
 */
public class HttpIndicesAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpIndicesAdminClient.class);

    private final HttpClient httpClient;

    private ValidateQueryActionHandler validateQueryActionHandler = new ValidateQueryActionHandler(this);
    private CreateIndexActionHandler createIndexActionHandler = new CreateIndexActionHandler(this);
    private DeleteIndexActionHandler deleteIndexActionHandler = new DeleteIndexActionHandler(this);
    private IndicesExistsActionHandler indicesExistsActionHandler = new IndicesExistsActionHandler(this);
    private RefreshActionHandler refreshActionHandler = new RefreshActionHandler(this);
    private FlushActionHandler flushActionHandler = new FlushActionHandler(this);
    private OptimizeActionHandler optimizeActionHandler = new OptimizeActionHandler(this);
    private OpenIndexActionHandler openIndexActionHandler = new OpenIndexActionHandler(this);
    private CloseIndexActionHandler closeIndexActionHandler = new CloseIndexActionHandler(this);
    private GetAliasesActionHandler getAliasesActionHandler = new GetAliasesActionHandler(this);

    public HttpIndicesAdminClient(HttpClient httpClient) {
        this.httpClient = httpClient;
        ImmutableMap.Builder<GenericAction, ActionHandler> tempActionHandlers = ImmutableMap.builder();
        tempActionHandlers.put(PutIndexTemplateAction.INSTANCE, new PutIndexTemplateActionHandler(this));
        tempActionHandlers.put(GetIndexTemplatesAction.INSTANCE, new GetTemplatesActionHandler(this));
        tempActionHandlers.put(UpdateSettingsAction.INSTANCE, new UpdateSettingsActionHandler(this));
        tempActionHandlers.put(GetMappingsAction.INSTANCE, new GetMappingsActionHandler(this));
        tempActionHandlers.put(GetSettingsAction.INSTANCE, new GetSettingsActionHandler(this));
        tempActionHandlers.put(IndicesAliasesAction.INSTANCE, new IndicesAliasesActionHandler(this));
//        tempActionHandlers.put(GetAliasesAction.INSTANCE, new GetAliasesActionHandler(this));
        tempActionHandlers.put(PutMappingAction.INSTANCE, new PutMappingActionHandler(this));
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }

    public void validateQuery(ValidateQueryRequest request, ActionListener<ValidateQueryResponse> listener) {
        validateQueryActionHandler.execute(request, listener);
    }

    public Future<ValidateQueryResponse> validateQuery(ValidateQueryRequest request) {
        PlainActionFuture<ValidateQueryResponse> future = PlainActionFuture.newFuture();
        validateQuery(request, future);
        return future;
    }

    public void createIndex(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener) {
        createIndexActionHandler.execute(request, listener);
    }

    public Future<CreateIndexResponse> createIndex(CreateIndexRequest request) {
        PlainActionFuture<CreateIndexResponse> future = PlainActionFuture.newFuture();
        createIndex(request, future);
        return future;
    }

    public void deleteIndex(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        deleteIndexActionHandler.execute(request, listener);
    }

    public Future<DeleteIndexResponse> deleteIndex(DeleteIndexRequest request) {
        PlainActionFuture<DeleteIndexResponse> future = PlainActionFuture.newFuture();
        deleteIndex(request, future);
        return future;
    }

    public void indexExists(IndicesExistsRequest request, ActionListener<IndicesExistsResponse> listener) {
        indicesExistsActionHandler.execute(request, listener);
    }

    public Future<IndicesExistsResponse> indexExists(IndicesExistsRequest request) {
        PlainActionFuture<IndicesExistsResponse> future = PlainActionFuture.newFuture();
        indexExists(request, future);
        return future;
    }

    public void refresh(RefreshRequest request, ActionListener<RefreshResponse> listener) {
        refreshActionHandler.execute(request, listener);
    }

    public Future<RefreshResponse> refresh(RefreshRequest request) {
        PlainActionFuture<RefreshResponse> future = PlainActionFuture.newFuture();
        refresh(request, future);
        return future;
    }

    public void flush(FlushRequest request, ActionListener<FlushResponse> listener) {
        flushActionHandler.execute(request, listener);
    }

    public Future<FlushResponse> flush(FlushRequest request) {
        PlainActionFuture<FlushResponse> future = PlainActionFuture.newFuture();
        flush(request, future);
        return future;
    }

    public void optimize(OptimizeRequest request, ActionListener<OptimizeResponse> listener) {
        optimizeActionHandler.execute(request, listener);
    }

    public Future<OptimizeResponse> optimize(OptimizeRequest request) {
        PlainActionFuture<OptimizeResponse> future = PlainActionFuture.newFuture();
        optimize(request, future);
        return future;
    }

    public void open(OpenIndexRequest request, ActionListener<OpenIndexResponse> listener) {
        openIndexActionHandler.execute(request, listener);
    }

    public Future<OpenIndexResponse> open(OpenIndexRequest request) {
        PlainActionFuture<OpenIndexResponse> future = PlainActionFuture.newFuture();
        open(request, future);
        return future;
    }

    public void close(CloseIndexRequest request, ActionListener<CloseIndexResponse> listener) {
        closeIndexActionHandler.execute(request, listener);
    }

    public Future<CloseIndexResponse> close(CloseIndexRequest request) {
        PlainActionFuture<CloseIndexResponse> future = PlainActionFuture.newFuture();
        close(request, future);
        return future;
    }

    public void getAliases(GetAliasesRequest request, ActionListener<GetAliasesResponse> listener) {
        getAliasesActionHandler.execute(request, listener);
    }

    public Future<GetAliasesResponse> getAliases(GetAliasesRequest request) {
        PlainActionFuture<GetAliasesResponse> future = PlainActionFuture.newFuture();
        getAliases(request, future);
        return future;
    }

}
