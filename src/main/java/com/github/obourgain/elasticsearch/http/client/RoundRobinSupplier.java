package com.github.obourgain.elasticsearch.http.client;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import com.github.obourgain.elasticsearch.http.concurrent.SnapshotableCopyOnWriteArray;
import com.google.common.base.Supplier;

public class RoundRobinSupplier<T> implements Supplier<T> {

    private final SnapshotableCopyOnWriteArray<T> clients;
    private final AtomicLong sequence = new AtomicLong();

    public RoundRobinSupplier(SnapshotableCopyOnWriteArray<T> clients) {
        this.clients = clients;
    }

    @Override
    public T get() {
        List<T> snapshot = clients.snapshot();
        if(snapshot.isEmpty()) {
            throw new IllegalStateException("no client available");
        }
        long next = sequence.getAndIncrement();
        int size = snapshot.size();
        T client = snapshot.get((int) (next % size));
        return client;
    }
}
