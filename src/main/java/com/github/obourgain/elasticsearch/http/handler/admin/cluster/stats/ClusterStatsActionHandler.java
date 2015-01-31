package com.github.obourgain.elasticsearch.http.handler.admin.cluster.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.client.HttpClusterAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerCompleterObserver;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
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
public class ClusterStatsActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ClusterStatsActionHandler.class);

    private final HttpClusterAdminClient indicesAdminClient;

    public ClusterStatsActionHandler(HttpClusterAdminClient httpClusterAdminClient) {
        this.indicesAdminClient = httpClusterAdminClient;
    }

    public ClusterStatsAction getAction() {
        return ClusterStatsAction.INSTANCE;
    }

    public void execute(ClusterStatsRequest request, final ActionListener<ClusterStatsResponse> listener) {
        logger.debug("cluster stats request {}", request);
        try {
            // TODO test
            String indices = Strings.arrayToCommaDelimitedString(request.nodesIds());
            if (!indices.isEmpty()) {
                indices = "/nodes/" + indices;
            }

            RequestUriBuilder uriBuilder = new RequestUriBuilder(indices)
                    .addEndpoint("_cluster/stats" + indices);

            indicesAdminClient.getHttpClient().client.submit(HttpClientRequest.createGet(uriBuilder.toString()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<ClusterStatsResponse>>() {
                        @Override
                        public Observable<ClusterStatsResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<ClusterStatsResponse>>() {
                                @Override
                                public Observable<ClusterStatsResponse> call(ByteBuf byteBuf) {
                                    return null;
//                                    return ClusterStatsResponse.parse(byteBuf);
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
