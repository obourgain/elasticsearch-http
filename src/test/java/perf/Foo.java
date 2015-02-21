package perf;

import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.functions.Action0;
import rx.functions.Action1;

public class Foo {

    public static final int ITERATIONS = 20_000;

    static final boolean resetClientBetweenWarmupAndRun = true;
    public static final int MAX_CONNECTIONS = 20_000;

    static CountDownLatch latch = new CountDownLatch(ITERATIONS);

    static final AtomicInteger count = new AtomicInteger();
    static final AtomicInteger error = new AtomicInteger();

    static volatile long start;

//    static final Semaphore semaphore = new Semaphore(1);

    public static void main(String[] args) throws Exception {

        HttpClient<ByteBuf, ByteBuf> client = createClient();

        startMonitorThread();

        System.out.println("warmup");
        executeQueries(client);
        latch.await();
        latch = new CountDownLatch(ITERATIONS);

        if (resetClientBetweenWarmupAndRun) {
            client.shutdown();
            client = createClient();
        }

        try {
            MILLISECONDS.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("bench");
        start = System.currentTimeMillis();
        executeQueries(client);
        latch.await();

        client.shutdown();
        System.out.println("waiting");
        latch.await();
        System.out.println("done in " + (System.currentTimeMillis() - start));
    }

    protected static HttpClient<ByteBuf, ByteBuf> createClient() {
        return RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder("localhost", 9200)
                //                .enableWireLogging(LogLevel.DEBUG)
                .withMaxConnections(MAX_CONNECTIONS)
                        //                .withNoConnectionPooling()
                .withNoIdleConnectionCleanup()
                .build();
    }

    protected static void executeQueries(final HttpClient<ByteBuf, ByteBuf> client) throws InterruptedException {
        for (int i = 0; i < ITERATIONS; i++) {
//            semaphore.acquire();
            client.submit(HttpClientRequest.createGet("/twitter2/foo/1"))
                    .onBackpressureBuffer(1000, new Action0() {
                        @Override
                        public void call() {
                            System.out.println("overflow");
                        }
                    })
                    .doOnError(new Action1<Throwable>() {
                        @Override
                        public void call(Throwable throwable) {
                            throwable.printStackTrace();
                            latch.countDown();
                            error.incrementAndGet();
                        }
                    })
                    .doOnCompleted(new Action0() {
                        @Override
                        public void call() {
//                            semaphore.release();
                            count.incrementAndGet();
                            latch.countDown();
                        }
                    }).forEach(new Action1<HttpClientResponse<ByteBuf>>() {
                @Override
                public void call(HttpClientResponse<ByteBuf> response) {
                }
            });
        }
    }

    protected static void startMonitorThread() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                while (true) {
                    long end = System.currentTimeMillis();
                    System.out.println(end - start);
                    System.out.println(count.get() + " " + error.get());
                    try {
                        MILLISECONDS.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        thread.setDaemon(true);
        thread.start();
    }
}
