package com.github.obourgain.elasticsearch.http.client;

import java.util.concurrent.Future;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.CreateIndexActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.GetMappingsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.GetTemplatesActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.IndicesExistsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.PutIndexTemplateActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.close.CloseIndexActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.delete.DeleteIndexActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.exists.IndicesAliasesActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.flush.FlushActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.getaliases.GetAliasesActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.mapping.put.PutMappingActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.open.OpenIndexActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.optimize.OptimizeActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.refresh.RefreshActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.settings.GetSettingsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.settings.UpdateSettingsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.validate.ValidateQueryActionHandler;
import com.github.obourgain.elasticsearch.http.handler.admin.indices.validate.ValidateQueryResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.close.CloseIndexResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.create.CreateIndexResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.delete.DeleteIndexResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.exists.IndicesExistsResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.flush.FlushResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.getaliases.GetAliasesResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.mapping.put.PutMappingResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.open.OpenIndexResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.optimize.OptimizeResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.refresh.RefreshResponse;
import com.github.obourgain.elasticsearch.http.response.admin.indices.template.put.PutIndexTemplateResponse;
import io.netty.buffer.ByteBuf;

/**
 * @author olivier bourgain
 */
public class HttpIndicesAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpIndicesAdminClient.class);

    private final io.reactivex.netty.protocol.http.client.HttpClient<ByteBuf, ByteBuf> httpClient;
    private ValidateQueryActionHandler validateQueryActionHandler = new ValidateQueryActionHandler(this);

    private CreateIndexActionHandler createIndexActionHandler = new CreateIndexActionHandler(this);
    private DeleteIndexActionHandler deleteIndexActionHandler = new DeleteIndexActionHandler(this);
    private IndicesExistsActionHandler indicesExistsActionHandler = new IndicesExistsActionHandler(this);
    private RefreshActionHandler refreshActionHandler = new RefreshActionHandler(this);
    private FlushActionHandler flushActionHandler = new FlushActionHandler(this);
    private OptimizeActionHandler optimizeActionHandler = new OptimizeActionHandler(this);
    private OpenIndexActionHandler openIndexActionHandler = new OpenIndexActionHandler(this);
    private CloseIndexActionHandler closeIndexActionHandler = new CloseIndexActionHandler(this);
    private PutMappingActionHandler putMappingActionHandler = new PutMappingActionHandler(this);
    private PutIndexTemplateActionHandler putIndexTemplateActionHandler = new PutIndexTemplateActionHandler(this);
    private GetTemplatesActionHandler getTemplatesActionHandler = new GetTemplatesActionHandler(this);
    private UpdateSettingsActionHandler updateSettingsActionHandler = new UpdateSettingsActionHandler(this);
    private GetSettingsActionHandler getSettingsActionHandler = new GetSettingsActionHandler(this);
    private GetMappingsActionHandler getMappingsActionHandler = new GetMappingsActionHandler(this);
    private IndicesAliasesActionHandler indicesAliasesActionHandler = new IndicesAliasesActionHandler(this);
    private GetAliasesActionHandler getAliasesActionHandler = new GetAliasesActionHandler(this);

    public HttpIndicesAdminClient(io.reactivex.netty.protocol.http.client.HttpClient<ByteBuf, ByteBuf> httpClient) {
        this.httpClient = httpClient;
    }

    public io.reactivex.netty.protocol.http.client.HttpClient<ByteBuf, ByteBuf> getHttpClient() {
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

    public void putMapping(PutMappingRequest request, ActionListener<PutMappingResponse> listener) {
        putMappingActionHandler.execute(request, listener);
    }

    public Future<PutMappingResponse> putMapping(PutMappingRequest request) {
        PlainActionFuture<PutMappingResponse> future = PlainActionFuture.newFuture();
        putMapping(request, future);
        return future;
    }

    public void putIndexTemplate(PutIndexTemplateRequest request, ActionListener<PutIndexTemplateResponse> listener) {
        putIndexTemplateActionHandler.execute(request, listener);
    }

    public Future<PutIndexTemplateResponse> putIndexTemplate(PutIndexTemplateRequest request) {
        PlainActionFuture<PutIndexTemplateResponse> future = PlainActionFuture.newFuture();
        putIndexTemplate(request, future);
        return future;
    }

    public void indicesExists(IndicesExistsRequest request, ActionListener<IndicesExistsResponse> listener) {
        indicesExistsActionHandler.execute(request, listener);
    }

    public Future<IndicesExistsResponse> indicesExists(IndicesExistsRequest request) {
        PlainActionFuture<IndicesExistsResponse> future = PlainActionFuture.newFuture();
        indicesExists(request, future);
        return future;
    }

}
