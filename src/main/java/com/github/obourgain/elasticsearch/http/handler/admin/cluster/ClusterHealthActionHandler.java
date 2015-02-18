package com.github.obourgain.elasticsearch.http.handler.admin.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClusterAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.request.RequestUriBuilder;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class ClusterHealthActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ClusterHealthActionHandler.class);

    private final HttpClusterAdminClient indicesAdminClient;

    public ClusterHealthActionHandler(HttpClusterAdminClient httpClusterAdminClient) {
        this.indicesAdminClient = httpClusterAdminClient;
    }

    public ClusterHealthAction getAction() {
        return ClusterHealthAction.INSTANCE;
    }

    public void execute(ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
        logger.debug("cluster health request {}", request);
        try {
            // TODO test
            StringBuilder url = new StringBuilder();
            url.append("_cluster/health");
            if (request.indices().length != 0) {
                url.append("/").append(Strings.arrayToCommaDelimitedString(request.indices()));
            }
            RequestUriBuilder uriBuilder = new RequestUriBuilder(url.toString());

            // TODO level ? http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/cluster-health.html#request-params
            uriBuilder.addQueryParameter("level", "shards");

            if (request.waitForStatus() != null) {
                uriBuilder.addQueryParameter("wait_for_status", request.waitForStatus().name().toLowerCase());
            }
            uriBuilder.addQueryParameterIfNotMinusOne("wait_for_relocating_shards", request.waitForRelocatingShards());
            if (!request.waitForNodes().equals("")) {
                uriBuilder.addQueryParameter("wait_for_nodes", request.waitForNodes());
            }
            uriBuilder.addQueryParameter("timeout", request.timeout().toString());
            uriBuilder.addQueryParameter("master_timeout", request.masterNodeTimeout().toString());

            indicesAdminClient.getHttpClient().client.submit(HttpClientRequest.createPut(uriBuilder.toString()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<ClusterHealthResponse>>() {
                        @Override
                        public Observable<ClusterHealthResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<ClusterHealthResponse>>() {
                                @Override
                                public Observable<ClusterHealthResponse> call(ByteBuf byteBuf) {
                                    // TODO
                                    return null;
                                }
                            });
                        }
                    })
                    .single()
                    .subscribe(new ListenerCompleterObserver<>(listener));

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
