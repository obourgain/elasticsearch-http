package com.github.obourgain.elasticsearch.http.client;

import java.util.Collection;
import java.util.concurrent.Future;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.exists.ExistsRequest;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.handler.document.bulk.BulkActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.bulk.BulkResponse;
import com.github.obourgain.elasticsearch.http.handler.document.delete.DeleteActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.delete.DeleteResponse;
import com.github.obourgain.elasticsearch.http.handler.document.deleteByQuery.DeleteByQueryActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.deleteByQuery.DeleteByQueryResponse;
import com.github.obourgain.elasticsearch.http.handler.document.get.GetActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.get.GetResponse;
import com.github.obourgain.elasticsearch.http.handler.document.index.IndexActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.index.IndexResponse;
import com.github.obourgain.elasticsearch.http.handler.document.morelikethis.MoreLikeThisActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.termvectors.TermVectorResponse;
import com.github.obourgain.elasticsearch.http.handler.document.termvectors.TermVectorsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.update.UpdateActionHandler;
import com.github.obourgain.elasticsearch.http.handler.document.update.UpdateResponse;
import com.github.obourgain.elasticsearch.http.handler.search.clearscroll.ClearScrollActionHandler;
import com.github.obourgain.elasticsearch.http.handler.search.clearscroll.ClearScrollResponse;
import com.github.obourgain.elasticsearch.http.handler.search.count.CountActionHandler;
import com.github.obourgain.elasticsearch.http.handler.search.count.CountResponse;
import com.github.obourgain.elasticsearch.http.handler.search.exists.ExistsActionHandler;
import com.github.obourgain.elasticsearch.http.handler.search.exists.ExistsResponse;
import com.github.obourgain.elasticsearch.http.handler.search.explain.ExplainActionHandler;
import com.github.obourgain.elasticsearch.http.handler.search.explain.ExplainResponse;
import com.github.obourgain.elasticsearch.http.handler.search.percolate.PercolateActionHandler;
import com.github.obourgain.elasticsearch.http.handler.search.percolate.PercolateResponse;
import com.github.obourgain.elasticsearch.http.handler.search.search.SearchActionHandler;
import com.github.obourgain.elasticsearch.http.handler.search.search.SearchResponse;
import com.github.obourgain.elasticsearch.http.url.RoundRobinUrlProviderStrategy;
import com.github.obourgain.elasticsearch.http.url.UrlProviderStrategy;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;

/**
 * @author olivier bourgain
 */
public class HttpClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpClient.class);

    private static final int DEFAULT_MAX_RETRIES = 0;
    private static final int DEFAULT_TIMEOUT_MILLIS = 30 * 1000 * 1000;

    public io.reactivex.netty.protocol.http.client.HttpClient<ByteBuf, ByteBuf> client;

    private int maxRetries = DEFAULT_MAX_RETRIES;
    private int timeOut = DEFAULT_TIMEOUT_MILLIS;
    private UrlProviderStrategy urlProviderStrategy;
    private HttpAdminClient httpAdminClient;

    IndexActionHandler indexActionHandler = new IndexActionHandler(this);
    GetActionHandler getActionHandler = new GetActionHandler(this);
    DeleteActionHandler deleteActionHandler = new DeleteActionHandler(this);
    UpdateActionHandler updateActionHandler = new UpdateActionHandler(this);
    DeleteByQueryActionHandler deleteByQueryActionHandler = new DeleteByQueryActionHandler(this);
    TermVectorsActionHandler termVectorActionHandler = new TermVectorsActionHandler(this);
    SearchActionHandler searchActionHandler = new SearchActionHandler(this);
    CountActionHandler countActionHandler = new CountActionHandler(this);
    ExistsActionHandler existsActionHandler = new ExistsActionHandler(this);
    ExplainActionHandler explainActionHandler = new ExplainActionHandler(this);
    PercolateActionHandler percolateActionHandler = new PercolateActionHandler(this);
    MoreLikeThisActionHandler moreLikeThisActionHandler = new MoreLikeThisActionHandler(this);
    ClearScrollActionHandler clearScrollActionHandler = new ClearScrollActionHandler(this);
    BulkActionHandler bulkActionHandler = new BulkActionHandler(this);
//    SuggestActionHandler suggestActionHandler = new SuggestActionHandler(this);

    public HttpClient(Collection<String> hosts) {
        this.urlProviderStrategy = new RoundRobinUrlProviderStrategy(hosts);

        // client
        // searchShard
        // search template
////        tempActionHandlers.put(MultiGetAction.INSTANCE, new MultiGetActionHandler(this));
//        tempActionHandlers.put(SearchScrollAction.INSTANCE, new SearchScrollActionHandler(this));

        // indices admin
        this.httpAdminClient = new HttpAdminClient(this);
        client = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder("localhost", 9501).build();
    }

    public void close() {
        client.shutdown();
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

    public void moreLikeThis(MoreLikeThisRequest request, ActionListener<SearchResponse> listener) {
        moreLikeThisActionHandler.execute(request, listener);
    }

    public Future<SearchResponse> moreLikeThis(MoreLikeThisRequest request) {
        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        moreLikeThis(request, future);
        return future;
    }

    public void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
        clearScrollActionHandler.execute(request, listener);
    }

    public Future<ClearScrollResponse> clearScroll(ClearScrollRequest request) {
        PlainActionFuture<ClearScrollResponse> future = PlainActionFuture.newFuture();
        clearScroll(request, future);
        return future;
    }

    public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        bulkActionHandler.execute(request, listener);
    }

    public Future<BulkResponse> bulk(BulkRequest request) {
        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        bulk(request, future);
        return future;
    }

//    public void suggest(SuggestRequest request, ActionListener<SuggestResponse> listener) {
//        suggestActionHandler.execute(request, listener);
//    }
//
//    public Future<SuggestResponse> suggest(SuggestRequest request) {
//        PlainActionFuture<SuggestResponse> future = PlainActionFuture.newFuture();
//        suggest(request, future);
//        return future;
//    }

}
