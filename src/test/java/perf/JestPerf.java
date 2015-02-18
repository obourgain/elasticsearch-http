package perf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;

public class JestPerf {

    public static final int ITERATIONS = 20_000;
    static ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 16, 60, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>());

    static final boolean resetClientBetweenWarmupAndRun = true;

    static CountDownLatch latch = new CountDownLatch(ITERATIONS);

    static List<JestClient> clients = Collections.synchronizedList(new ArrayList<JestClient>());

    static ThreadLocal<JestClient> client = new ThreadLocal<JestClient>() {
        @Override
        protected JestClient initialValue() {
            HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://localhost:9200").build();

            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(clientConfig);
            JestClient client = factory.getObject();
            clients.add(client);
            return client;
        }
    };

    public static void main(String[] args) throws Exception {
        System.out.println("warmup");
        executeQueries();
        latch.await();
        latch = new CountDownLatch(ITERATIONS);

        if(resetClientBetweenWarmupAndRun) {
            resetClients();
        }

        System.out.println("bench");
        long start = System.currentTimeMillis();
        executeQueries();

        latch.await();
        long end = System.currentTimeMillis();
        System.out.println(end - start);

        executor.shutdown();
    }

    protected static void executeQueries() {
        for (int i = 0; i < ITERATIONS; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        client.get().execute(new Get.Builder("twitter2", "1").type("foo").build()).getJsonString();
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    private static void resetClients() {
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        executor = new ThreadPoolExecutor(16, 16, 60, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>());
    }
}
