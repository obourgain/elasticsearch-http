package com.github.obourgain.elasticsearch.http.handler.admin.cluster.node.hotthreads;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.obourgain.elasticsearch.http.HttpClientImpl;
import com.github.obourgain.elasticsearch.http.admin.HttpClusterAdminClient;
import com.github.obourgain.elasticsearch.http.concurrent.ListenerAsyncCompletionHandler;
import com.github.obourgain.elasticsearch.http.handler.ActionHandler;
import com.github.obourgain.elasticsearch.http.response.ResponseWrapper;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

/**
 * @author olivier bourgain
 */
public class NodesHotThreadsActionHandler implements ActionHandler<NodesHotThreadsRequest, NodesHotThreadsResponse, NodesHotThreadsRequestBuilder> {

    private static final Logger logger = LoggerFactory.getLogger(NodesHotThreadsActionHandler.class);

    private final HttpClusterAdminClient indicesAdminClient;

    public NodesHotThreadsActionHandler(HttpClusterAdminClient httpClusterAdminClient) {
        this.indicesAdminClient = httpClusterAdminClient;
    }

    @Override
    public NodesHotThreadsAction getAction() {
        return NodesHotThreadsAction.INSTANCE;
    }

    @Override
    public void execute(NodesHotThreadsRequest request, final ActionListener<NodesHotThreadsResponse> listener) {
        logger.debug("nodes hot threads request {}", request);
        try {
            HttpClientImpl httpClient = indicesAdminClient.getHttpClient();
            StringBuilder url = new StringBuilder();
            url.append(httpClient.getUrl());
            url.append("/_nodes/");
            if(request.nodesIds().length != 0) {
                url.append(Strings.arrayToCommaDelimitedString(request.nodesIds()));
                url.append("/");
            }
            url.append("hot_threads");

            TimeValue interval = request.interval();
            int snapshots = request.snapshots();
            int threads = request.threads();
            String type = request.type();

            AsyncHttpClient.BoundRequestBuilder httpRequest = httpClient.asyncHttpClient.prepareGet(url.toString());

            httpRequest.addQueryParam("interval", interval.toString());
            httpRequest.addQueryParam("snapshots", String.valueOf(snapshots));
            httpRequest.addQueryParam("threads", String.valueOf(threads));
            httpRequest.addQueryParam("type", type);

            httpRequest.execute(new ListenerAsyncCompletionHandler<NodesHotThreadsResponse>(listener) {
                @Override
                public NodesHotThreadsResponse onCompleted(Response response) {
                    // this is not a json response, so I can not use the classic mecanism
                    // TODO use indexOf to find the first lines and avoid parsing the full body
                    List<String> splitted;
                    try {
                        splitted = Lists.newArrayList(Splitter.on(":::").omitEmptyStrings().split(response.getResponseBody()));
                    } catch (IOException e) {
                        throw new RuntimeException("Unable to read node hot thread response", e);
                    }

                    //  0.4% (2ms out of 500ms) cpu usage by thread 'elasticsearch[transport_client_node_1][transport_client_worker][T#1]{New I/O worker #205}'
                    // 10/10 snapshots sharing following 15 elements
                    // sun.nio.ch.EPollArrayWrapper.epollWait(Native Method)
                    // [...stack trace ...]

                    Iterable<NodeHotThreads> transformed = Iterables.transform(splitted, new Function<String, NodeHotThreads>() {
                        @Override
                        public NodeHotThreads apply(String input) {
                            // ::: [node_1][nh_NJHcRRuameGupZWeCig][olivier-pc][inet[/10.1.103.89:9301]]
                            // TODO the toString() from DiscoveryNode may omit some elements
                            List<String> parts = Lists.newArrayList(Splitter.on("[").split(input));
                            String nodeName = parts.get(1).replace("]", ""); // remove trailing ']'
                            String nodeId = parts.get(2).replace("]", "");
//                            String hostname = parts.get(3).replace("]", "");
//                            String transportType = parts.get(4);
                            // TODO clean this
                            // TODO move the trailing ']' handling to parseTransportAddress
                            String host = parts.get(5).substring(0, parts.get(5).indexOf(']'));
                            String address = parts.get(4) + host;
                            TransportAddress transportAddress = ResponseWrapper.parseTransportAddress(address);
                            DiscoveryNode node = new DiscoveryNode(nodeName, nodeId, transportAddress, Collections.<String, String>emptyMap(), Version.CURRENT);
                            return new NodeHotThreads(node, input);
                        }
                    });
                    NodeHotThreads[] nodeHotThreadses = FluentIterable.from(transformed).toArray(NodeHotThreads.class);
                    NodesHotThreadsResponse nodeHotThreads = new NodesHotThreadsResponse(new ClusterName(""), nodeHotThreadses);
                    listener.onResponse(nodeHotThreads);
                    return nodeHotThreads;
                }

                @Override
                protected NodesHotThreadsResponse convert(ResponseWrapper responseWrapper) {
//                    return responseWrapper.toNodesHotThreadsResponse();
                    throw new IllegalStateException("should not be called");
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
