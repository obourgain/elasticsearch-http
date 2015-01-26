package perf;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;

public class JestPerf {

    public static final int ITERATIONS = 100000;
    static ThreadPoolExecutor executor = new ThreadPoolExecutor(16, 16, 60, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>());

    static CountDownLatch latch = new CountDownLatch(ITERATIONS);
    static CountDownLatch warmuplatch = new CountDownLatch(ITERATIONS);

    static ThreadLocal<JestClient> client = new ThreadLocal<JestClient>() {
        @Override
        protected JestClient initialValue() {
            HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://localhost:9200").build();

            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(clientConfig);
            return  factory.getObject();
        }
    };

    public static void main(String[] args) throws Exception {


        for (int i = 0; i < ITERATIONS; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        client.get().execute(new Get.Builder("twitter2", "1").type("foo").build()).getJsonString();
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
                        client.get().execute(new Get.Builder("twitter2", "1").type("foo").build()).getJsonString();
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
