package com.github.obourgain.elasticsearch.http.url;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;

/**
 * @author olivier bourgain
 */
public class RoundRobinUrlProviderStrategy implements UrlProviderStrategy {

    private final Collection<String> urls;
    private final Iterator<String> iterator;

    private final Object monitor = new Object();

    public RoundRobinUrlProviderStrategy(Collection<String> urls) {
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
        this.iterator = Iterators.cycle(prefixedUrls);
        // iterator is final so publication is safe, no need to synchronize to ensure visibility
    }

    @Override
    public String getUrl() {
        synchronized (monitor) {
            return iterator.next();
        }
    }

    @Override
    public String toString() {
        return "RoundRobinUrlProviderStrategy{" +
                "urls=" + urls +
                '}';
    }
}
