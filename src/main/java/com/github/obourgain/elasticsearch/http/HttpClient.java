package com.github.obourgain.elasticsearch.http;

import java.util.Collection;
import java.util.concurrent.Future;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.admin.HttpAdminClient;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.DeleteActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.DeleteByQueryActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.GetActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.IndexActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.TermVectorsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.UpdateActionHandler;
import com.github.obourgain.elasticsearch.http.handler.search.CountActionHandler;
import com.github.obourgain.elasticsearch.http.handler.search.ExistsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.search.ExplainActionHandler;
import com.github.obourgain.elasticsearch.http.handler.search.PercolateActionHandler;
import com.github.obourgain.elasticsearch.http.handler.search.SearchActionHandler;
import com.github.obourgain.elasticsearch.http.response.count.CountResponse;
import com.github.obourgain.elasticsearch.http.response.delete.DeleteResponse;
import com.github.obourgain.elasticsearch.http.response.deleteByQuery.DeleteByQueryResponse;
import com.github.obourgain.elasticsearch.http.response.exists.ExistsResponse;
import com.github.obourgain.elasticsearch.http.response.explain.ExplainResponse;
import com.github.obourgain.elasticsearch.http.response.get.GetResponse;
import com.github.obourgain.elasticsearch.http.response.index.IndexResponse;
import com.github.obourgain.elasticsearch.http.response.percolate.PercolateResponse;
import com.github.obourgain.elasticsearch.http.response.search.SearchResponse;
import com.github.obourgain.elasticsearch.http.response.termvectors.TermVectorResponse;
import com.github.obourgain.elasticsearch.http.response.update.UpdateResponse;
import com.github.obourgain.elasticsearch.http.url.RoundRobinUrlProviderStrategy;
import com.github.obourgain.elasticsearch.http.url.UrlProviderStrategy;
import com.google.common.collect.ImmutableMap;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;

/**
 * @author olivier bourgain
 */
public class HttpClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpClient.class);

    private static final int DEFAULT_MAX_RETRIES = 0;
    private static final int DEFAULT_TIMEOUT_MILLIS = 30 * 1000;

    public AsyncHttpClient asyncHttpClient;

    private int maxRetries = DEFAULT_MAX_RETRIES;
    private int timeOut = DEFAULT_TIMEOUT_MILLIS;
    private UrlProviderStrategy urlProviderStrategy;
    private HttpAdminClient httpAdminClient;

    private IndexActionHandler indexActionHandler = new IndexActionHandler(this);
    private GetActionHandler getActionHandler = new GetActionHandler(this);
    private DeleteActionHandler deleteActionHandler = new DeleteActionHandler(this);
    private UpdateActionHandler updateActionHandler = new UpdateActionHandler(this);
    private DeleteByQueryActionHandler deleteByQueryActionHandler = new DeleteByQueryActionHandler(this);
    private TermVectorsActionHandler termVectorActionHandler = new TermVectorsActionHandler(this);
    private SearchActionHandler searchActionHandler = new SearchActionHandler(this);
    private CountActionHandler countActionHandler = new CountActionHandler(this);
    private ExistsActionHandler existsActionHandler = new ExistsActionHandler(this);
    private ExplainActionHandler explainActionHandler = new ExplainActionHandler(this);
    private PercolateActionHandler percolateActionHandler = new PercolateActionHandler(this);

    public HttpClient(Collection<String> hosts) {
        this.urlProviderStrategy = new RoundRobinUrlProviderStrategy(hosts);

        AsyncHttpClientConfig config = new AsyncHttpClientConfig.Builder()
                .setMaxRequestRetry(maxRetries)
                .setConnectTimeout(timeOut)
                .setRequestTimeout(timeOut)
                        // TODO lots of options ...
                .build();

        asyncHttpClient = new AsyncHttpClient(config);

        ImmutableMap.Builder<GenericAction, ActionHandler> tempActionHandlers = ImmutableMap.builder();
        // client
        // searchShard
        // search template
////        tempActionHandlers.put(MultiGetAction.INSTANCE, new MultiGetActionHandler(this));
//        tempActionHandlers.put(MoreLikeThisAction.INSTANCE, new MoreLikeThisActionHandler(this));
//        tempActionHandlers.put(BulkAction.INSTANCE, new BulkActionHandler(this));

        // indices admin
        this.httpAdminClient = new HttpAdminClient(this);
    }

    public void close() {
        asyncHttpClient.close();
    }

    public HttpAdminClient admin() {
        return httpAdminClient;
    }

    public String getUrl() {
        return urlProviderStrategy.getUrl();
    }

    public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        indexActionHandler.execute(request, listener);
    }

    public Future<IndexResponse> index(IndexRequest request) {
        PlainActionFuture<IndexResponse> future = PlainActionFuture.newFuture();
        index(request, future);
        return future;
    }

    public void get(GetRequest request, ActionListener<GetResponse> listener) {
        getActionHandler.execute(request, listener);
    }

    public Future<GetResponse> get(GetRequest request) {
        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        get(request, future);
        return future;
    }

    public void delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        deleteActionHandler.execute(request, listener);
    }

    public Future<DeleteResponse> delete(DeleteRequest request) {
        PlainActionFuture<DeleteResponse> future = PlainActionFuture.newFuture();
        delete(request, future);
        return future;
    }

    public void update(UpdateRequest request, ActionListener<UpdateResponse> listener) {
        updateActionHandler.execute(request, listener);
    }

    public Future<UpdateResponse> update(UpdateRequest request) {
        PlainActionFuture<UpdateResponse> future = PlainActionFuture.newFuture();
        update(request, future);
        return future;
    }

    public void deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        deleteByQueryActionHandler.execute(request, listener);
    }

    public Future<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request) {
        PlainActionFuture<DeleteByQueryResponse> future = PlainActionFuture.newFuture();
        deleteByQuery(request, future);
        return future;
    }

    public void termVectors(TermVectorRequest request, ActionListener<TermVectorResponse> listener) {
        termVectorActionHandler.execute(request, listener);
    }

    public Future<TermVectorResponse> termVectors(TermVectorRequest request) {
        PlainActionFuture<TermVectorResponse> future = PlainActionFuture.newFuture();
        termVectors(request, future);
        return future;
    }

    public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        searchActionHandler.execute(request, listener);
    }

    public Future<SearchResponse> search(SearchRequest request) {
        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        search(request, future);
        return future;
    }

    public void count(CountRequest request, ActionListener<CountResponse> listener) {
        countActionHandler.execute(request, listener);
    }

    public Future<CountResponse> count(CountRequest request) {
        PlainActionFuture<CountResponse> future = PlainActionFuture.newFuture();
        count(request, future);
        return future;
    }

    public void exists(ExistsRequest request, ActionListener<ExistsResponse> listener) {
        existsActionHandler.execute(request, listener);
    }

    public Future<ExistsResponse> exists(ExistsRequest request) {
        PlainActionFuture<ExistsResponse> future = PlainActionFuture.newFuture();
        exists(request, future);
        return future;
    }

    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        explainActionHandler.execute(request, listener);
    }

    public Future<ExplainResponse> explain(ExplainRequest request) {
        PlainActionFuture<ExplainResponse> future = PlainActionFuture.newFuture();
        explain(request, future);
        return future;
    }

    public void percolate(PercolateRequest request, ActionListener<PercolateResponse> listener) {
        percolateActionHandler.execute(request, listener);
    }

    public Future<PercolateResponse> percolate(PercolateRequest request) {
        PlainActionFuture<PercolateResponse> future = PlainActionFuture.newFuture();
        percolate(request, future);
        return future;
    }

}
