package com.github.obourgain.elasticsearch.http.handler.admin.cluster.node.hotthreads;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
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
public class NodesHotThreadsActionHandler {

    private static final Logger logger = LoggerFactory.getLogger(NodesHotThreadsActionHandler.class);

    private final HttpClusterAdminClient indicesAdminClient;

    public NodesHotThreadsActionHandler(HttpClusterAdminClient httpClusterAdminClient) {
        this.indicesAdminClient = httpClusterAdminClient;
    }

    public NodesHotThreadsAction getAction() {
        return NodesHotThreadsAction.INSTANCE;
    }

    public void execute(NodesHotThreadsRequest request, final ActionListener<NodesHotThreadsResponse> listener) {
        logger.debug("nodes hot threads request {}", request);
        // TODO test
        try {
            StringBuilder url = new StringBuilder();
            url.append("_nodes/");
            if (request.nodesIds().length != 0) {
                url.append(Strings.arrayToCommaDelimitedString(request.nodesIds()));
            }
            RequestUriBuilder uriBuilder = new RequestUriBuilder(url.toString());
            uriBuilder.addEndpoint("hot_threads");

            TimeValue interval = request.interval();
            int snapshots = request.snapshots();
            int threads = request.threads();
            String type = request.type();

            uriBuilder.addQueryParameter("interval", interval.toString());
            uriBuilder.addQueryParameter("snapshots", String.valueOf(snapshots));
            uriBuilder.addQueryParameter("threads", String.valueOf(threads));
            uriBuilder.addQueryParameter("type", type);

            indicesAdminClient.getHttpClient().submit(HttpClientRequest.createGet(uriBuilder.toString()))
                    .flatMap(ErrorHandler.AS_FUNC)
                    .flatMap(new Func1<HttpClientResponse<ByteBuf>, Observable<NodesHotThreadsResponse>>() {
                        @Override
                        public Observable<NodesHotThreadsResponse> call(HttpClientResponse<ByteBuf> response) {
                            return response.getContent().flatMap(new Func1<ByteBuf, Observable<NodesHotThreadsResponse>>() {
                                @Override
                                public Observable<NodesHotThreadsResponse> call(ByteBuf byteBuf) {
                                    // TODO
                                    return null;
                                }
                            });
                        }
                    })
                    .single()
                    .subscribe(new ListenerCompleterObserver<>(listener));

//                public NodesHotThreadsResponse onCompleted(Response response) {
//                    // this is not a json response, so I can not use the classic mecanism
//                    // TODO use indexOf to find the first lines and avoid parsing the full body
//                    List<String> splitted;
//                    try {
//                        splitted = Lists.newArrayList(Splitter.on(":::").omitEmptyStrings().split(response.getResponseBody()));
//                    } catch (IOException e) {
//                        throw new RuntimeException("Unable to read node hot thread response", e);
//                    }
//
//                    //  0.4% (2ms out of 500ms) cpu usage by thread 'elasticsearch[transport_client_node_1][transport_client_worker][T#1]{New I/O worker #205}'
//                    // 10/10 snapshots sharing following 15 elements
//                    // sun.nio.ch.EPollArrayWrapper.epollWait(Native Method)
//                    // [...stack trace ...]
//
//                    Iterable<NodeHotThreads> transformed = Iterables.transform(splitted, new Function<String, NodeHotThreads>() {
//                        @Override
//                        public NodeHotThreads apply(String input) {
//                            // ::: [node_1][nh_NJHcRRuameGupZWeCig][olivier-pc][inet[/10.1.103.89:9301]]
//                            // TODO the toString() from DiscoveryNode may omit some elements
//                            List<String> parts = Lists.newArrayList(Splitter.on("[").split(input));
//                            String nodeName = parts.get(1).replace("]", ""); // remove trailing ']'
//                            String nodeId = parts.get(2).replace("]", "");
////                            String hostname = parts.get(3).replace("]", "");
////                            String transportType = parts.get(4);
//                            // TODO clean this
//                            // TODO move the trailing ']' handling to parseTransportAddress
//                            String host = parts.get(5).substring(0, parts.get(5).indexOf(']'));
//                            String address = parts.get(4) + host;
//                            TransportAddress transportAddress = ResponseWrapper.parseTransportAddress(address);
//                            DiscoveryNode node = new DiscoveryNode(nodeName, nodeId, transportAddress, Collections.<String, String>emptyMap(), Version.CURRENT);
//                            return new NodeHotThreads(node, input);
//                        }
//                    });
//                    NodeHotThreads[] nodeHotThreadses = FluentIterable.from(transformed).toArray(NodeHotThreads.class);
//                    NodesHotThreadsResponse nodeHotThreads = new NodesHotThreadsResponse(new ClusterName(""), nodeHotThreadses);

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
