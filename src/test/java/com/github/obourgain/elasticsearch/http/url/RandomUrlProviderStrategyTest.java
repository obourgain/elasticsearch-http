package com.github.obourgain.elasticsearch.http.url;

import static org.junit.Assert.assertFalse;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class RandomUrlProviderStrategyTest {

    @Test
    public void checkThreadSafety() throws InterruptedException {
        // do our best to try to make it fail
        final UrlProviderStrategy strategy = new RandomUrlProviderStrategy(Collections.singletonList("url"));

        int threadNumber = 10;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch end = new CountDownLatch(threadNumber);

        final AtomicBoolean failed = new AtomicBoolean(false);

        for (int i = 0; i < threadNumber; i++) {
            new Thread() {
                @Override
                public void run() {
                    try {
                        start.await();
                        for (int j = 0; j < 1000000; j++) {
                            // this should neot be subject to DCE because we have a memory barrier because of the volatile read in the AtomicInteger
                            strategy.getUrl();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                        failed.set(true);
                    } finally {
                        end.countDown();
                    }
                }
            }.start();
        }
        start.countDown();
        end.await();

        assertFalse(failed.get());
    }

}