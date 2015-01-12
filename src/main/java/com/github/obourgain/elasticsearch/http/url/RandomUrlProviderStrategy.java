package com.github.obourgain.elasticsearch.http.url;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

/**
 * @author olivier bourgain
 */
public class RandomUrlProviderStrategy implements UrlProviderStrategy {

    private final List<String> urls;

    private final AtomicInteger randomNodeGenerator = new AtomicInteger();

    public RandomUrlProviderStrategy(Collection<String> urls) {
        Preconditions.checkState(!urls.isEmpty(), "Provided URL list shall not be empty");
        Collection<String> prefixedUrls = Collections2.transform(urls, new Function<String, String>() {
            @Override
            public String apply(String input) {
                if (input.startsWith("http")) {
                    return input;
                }
                // TODO can do better ?
                return "http://" + input;
            }
        });
        this.urls = new ArrayList<>(prefixedUrls);
    }

    @Override
    public String getUrl() {
        int index = randomNodeGenerator.incrementAndGet();
        if (index < 0) {
            index = 0;
            randomNodeGenerator.set(0);
        }
        return urls.get(index % urls.size());
    }

    @Override
    public String toString() {
        return "RandomUrlProviderStrategy{" +
                "urls=" + urls +
                '}';
    }
}
