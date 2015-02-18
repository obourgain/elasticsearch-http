package perf;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;

public class TransportClientPerf {

    public static final int ITERATIONS = 20_000;
    static ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 16, 60, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>());

    static final boolean resetClientBetweenWarmupAndRun = true;

    static CountDownLatch latch = new CountDownLatch(ITERATIONS);

    static volatile long start;

    public static void main(String[] args) throws Exception {
        TransportClient client = createClient();

        System.out.println("warmup");
        executeQueries(client);
        latch.await();
        latch = new CountDownLatch(ITERATIONS);

        if(resetClientBetweenWarmupAndRun) {
            client.close();
        }

        client = createClient();
        System.out.println("bench");
        start = System.currentTimeMillis();
        executeQueries(client);
        latch.await();

        client.close();
        System.out.println("waiting");
        latch.await();
        System.out.println("done in " + (System.currentTimeMillis() - start));

        latch.await();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        client.close();
        executor.shutdown();
    }

    private static TransportClient createClient() {
        return new TransportClient(ImmutableSettings.builder().put("cluster.name", "elasticsearch")).addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
    }

    private static void executeQueries(final TransportClient client) {
        for (int i = 0; i < ITERATIONS; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        client.get(new GetRequest("twitter2", "foo", "1")).actionGet();
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
