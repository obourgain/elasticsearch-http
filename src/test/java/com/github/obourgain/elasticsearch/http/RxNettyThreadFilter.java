package com.github.obourgain.elasticsearch.http;

import java.util.Set;
import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.google.common.collect.ImmutableSet;

public class RxNettyThreadFilter implements ThreadFilter {

    private static final Set<String> WELL_KNOWN_NAMES = ImmutableSet.<String>builder()
            .add("RxComputationThreadPool")
            .add("rxnetty-nio-eventloop")
            .add("global-client-idle-conn-cleanup-scheduler")
            .add("threadDeathWatcher")
            .build();

    @Override
    public boolean reject(Thread t) {
        for (String name : WELL_KNOWN_NAMES) {
            if(t.getName().startsWith(name)) {
                return true;
            }
        }
        return false;
    }
}
