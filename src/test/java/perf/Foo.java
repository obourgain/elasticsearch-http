package perf;

import static java.lang.Long.MAX_VALUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import rx.functions.Action1;

public class Foo {

    public static final int ITERATIONS = 10000;
    static CountDownLatch latch = new CountDownLatch(ITERATIONS);

    static AtomicInteger count = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        HttpClient<ByteBuf, ByteBuf> client = RxNetty.<ByteBuf, ByteBuf>newHttpClientBuilder("localhost", 9200)
                .enableWireLogging(LogLevel.DEBUG)
                .withMaxConnections(100000)
                .withNoConnectionPooling()
//                .withNoIdleConnectionCleanup()
                .build();

        new Thread() {
            @Override
            public void run() {
                while(true) {
                    System.out.println(count.get());
                    try {
                        MILLISECONDS.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }.start();

        for (int i = 0; i < ITERATIONS; i++) {
            final int local = i;
            client.submit(HttpClientRequest.createGet("/twitter2/foo/1")).forEach(new Action1<HttpClientResponse<ByteBuf>>() {
                @Override
                public void call(HttpClientResponse<ByteBuf> byteBufHttpClientResponse) {
                    System.out.println("done " + local);
                    count.incrementAndGet();
                    latch.countDown();
                }
            });
        }

        System.out.println("waiting");
        latch.await();
        System.out.println("done");
    }


}
