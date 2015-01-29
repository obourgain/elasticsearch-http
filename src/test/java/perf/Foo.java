package perf;

import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.functions.Action0;
import rx.functions.Action1;

public class Foo {

    public static final int ITERATIONS = 10000;
    static CountDownLatch latch = new CountDownLatch(ITERATIONS);

    static AtomicInteger count = new AtomicInteger();
    static AtomicInteger error = new AtomicInteger();

    static final AtomicLong start = new AtomicLong();

    public static void main(String[] args) throws Exception {

        HttpClient<ByteBuf, ByteBuf> client = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder("localhost", 9200)
//                .enableWireLogging(LogLevel.DEBUG)
                .withMaxConnections(100)
//                .withNoConnectionPooling()
                .withNoIdleConnectionCleanup()
                .build();

        new Thread() {
            @Override
            public void run() {
                while(true) {
                    long end = System.currentTimeMillis();
                    System.out.println(end - start.get());
                    System.out.println(count.get() + " " + error.get());
                    try {
                        MILLISECONDS.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }.start();
        start.set(System.currentTimeMillis());
        for (int i = 0; i < ITERATIONS; i++) {
//            try {
//                MILLISECONDS.sleep(0);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
            final int local = i;
            client.submit(HttpClientRequest.createGet("/twitter2/foo/1"))
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
                            count.incrementAndGet();
                            latch.countDown();
                        }
                    })
                    .forEach(new Action1<HttpClientResponse<ByteBuf>>() {
                        @Override
                        public void call(HttpClientResponse<ByteBuf> byteBufHttpClientResponse) {
//                            System.out.println("done " + local);
//                            count.incrementAndGet();
//                            latch.countDown();
                        }
                    });
        }

        client.shutdown();
        System.out.println("waiting");
        latch.await();
        System.out.println("done");
    }


}
