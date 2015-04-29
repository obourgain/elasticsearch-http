package com.github.obourgain.elasticsearch.http.client;

import static org.junit.Assert.fail;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import com.github.obourgain.elasticsearch.http.concurrent.SnapshotableCopyOnWriteArray;

public class RoundRobinSupplierTest {

    @Test
    public void should_supply() throws Exception {
        SnapshotableCopyOnWriteArray<String> array = new SnapshotableCopyOnWriteArray<>();
        array.add("foo");
        RoundRobinSupplier<String> supplier = new RoundRobinSupplier<>(array);

        Assertions.assertThat(supplier.get()).isNotNull();
    }

    @Test
    public void should_iterate_through_values() throws Exception {
        SnapshotableCopyOnWriteArray<String> array = new SnapshotableCopyOnWriteArray<>();
        array.add("foo");
        array.add("bar");

        RoundRobinSupplier<String> supplier = new RoundRobinSupplier<>(array);

        Assertions.assertThat(supplier.get()).isEqualTo("foo");
        Assertions.assertThat(supplier.get()).isEqualTo("bar");
    }

    @Test
    public void should_loop_after_the_end() throws Exception {
        SnapshotableCopyOnWriteArray<String> array = new SnapshotableCopyOnWriteArray<>();
        array.add("foo");
        array.add("bar");

        RoundRobinSupplier<String> supplier = new RoundRobinSupplier<>(array);

        Assertions.assertThat(supplier.get()).isEqualTo("foo");
        Assertions.assertThat(supplier.get()).isEqualTo("bar");
        // loop
        Assertions.assertThat(supplier.get()).isEqualTo("foo");
        Assertions.assertThat(supplier.get()).isEqualTo("bar");
    }

    @Test
    public void should_throw_if_empty() throws Exception {
        SnapshotableCopyOnWriteArray<String> array = new SnapshotableCopyOnWriteArray<>();
        RoundRobinSupplier<String> supplier = new RoundRobinSupplier<>(array);

        try {
            supplier.get();
            fail();
        } catch (IllegalStateException e) {
            Assertions.assertThat(e).hasMessage("no client available");
        }
    }

}