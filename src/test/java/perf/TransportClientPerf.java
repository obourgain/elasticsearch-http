package perf;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;

public class TransportClientPerf {

    public static final int ITERATIONS = 100_000;
    static ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 16, 60, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>());

    static CountDownLatch latch = new CountDownLatch(ITERATIONS);
    static CountDownLatch warmuplatch = new CountDownLatch(ITERATIONS);

    public static void main(String[] args) throws Exception {
        final TransportClient client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        client.get(new GetRequest("twitter2", "foo", "1")).actionGet();
        client.get(new GetRequest("twitter2", "foo", "1")).actionGet();
        client.get(new GetRequest("twitter2", "foo", "1")).actionGet();

        for (int i = 0; i < ITERATIONS; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
//                        System.out.println("warm");
                        client.get(new GetRequest("twitter2", "foo", "1")).actionGet();
                        warmuplatch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        warmuplatch.await();

        long start = System.currentTimeMillis();
        for (int i = 0; i < ITERATIONS; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
//                        System.out.println("run");
                        client.get(new GetRequest("twitter2", "foo", "1")).actionGet();
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        latch.await();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
