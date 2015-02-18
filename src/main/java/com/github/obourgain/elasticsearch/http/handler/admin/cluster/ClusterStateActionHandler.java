package com.github.obourgain.elasticsearch.http.handler.admin.cluster;

import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClusterAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.response.ErrorHandler;
import com.google.common.base.Joiner;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author olivier bourgain
 */
public class ClusterStateActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ClusterStateActionHandler.class);

    private final HttpClusterAdminClient indicesAdminClient;

    public ClusterStateActionHandler(HttpClusterAdminClient httpClusterAdminClient) {
        this.indicesAdminClient = httpClusterAdminClient;
    }

    public ClusterStateAction getAction() {
        return ClusterStateAction.INSTANCE;
    }

    public void execute(ClusterStateRequest request, final ActionListener<ClusterStateResponse> listener) {
        logger.debug("cluster state request {}", request);
        try {
            // TODO test

            List<String> metricsAsList = new ArrayList<>();
            if (request.blocks()) {
                metricsAsList.add("blocks");
            }
            if (request.routingTable()) {
                metricsAsList.add("routing_table");
            }
            if (request.nodes()) {
                metricsAsList.add("nodes");
            }
            if (request.metaData()) {
                metricsAsList.add("metadata");
            }

            String metrics;
            if (metricsAsList.isEmpty()) {
                metrics = "_all";
            } else {
                // TODO version and master_node are not in the request, so add them by default
                metricsAsList.add("version");
                metricsAsList.add("master_node");
                metrics = Joiner.on(",").join(metricsAsList);
            }

            String indices = Strings.arrayToCommaDelimitedString(request.indices());

            String uri = "/_cluster/state/" + metrics + "/" + indices;

            indicesAdminClient.getHttpClient().client.submit(HttpClientRequest.createPut(uri))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<ClusterStateResponse>>() {
                        @Override
                        public Observable<ClusterStateResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<ClusterStateResponse>>() {
                                @Override
                                public Observable<ClusterStateResponse> call(ByteBuf byteBuf) {
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
