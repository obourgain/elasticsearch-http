package com.github.obourgain.elasticsearch.http.concurrent;

import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class SnapshotableCopyOnWriteArrayTest {

    @Test
    public void should_add() throws Exception {
        SnapshotableCopyOnWriteArray<Object> array = new SnapshotableCopyOnWriteArray<>();
        array.add("foo");
        Assertions.assertThat(array.size()).isEqualTo(1);
    }

    @Test
    public void should_add_all() {
        SnapshotableCopyOnWriteArray<Object> array = new SnapshotableCopyOnWriteArray<>();
        array.addAll(Arrays.asList("foo", "bar"));
        Assertions.assertThat(array.size()).isEqualTo(2);
    }

    @Test
    public void should_add_all_to_non_emtpy_array() {
        SnapshotableCopyOnWriteArray<Object> array = new SnapshotableCopyOnWriteArray<>();
        array.add("hello");
        array.addAll(Arrays.asList("foo", "bar"));
        Assertions.assertThat(array.size()).isEqualTo(3);
    }

    @Test
    public void should_create_a_snapshot() {
        SnapshotableCopyOnWriteArray<Object> array = new SnapshotableCopyOnWriteArray<>();
        array.addAll(Arrays.asList("foo", "bar"));
        Assertions.assertThat(array.size()).isEqualTo(2);

        List<Object> snapshot = array.snapshot();
        Assertions.assertThat(snapshot).containsExactly("foo", "bar");
    }

    @Test
    public void snapshot_should_not_be_modified_when_adding_after_it() {
        SnapshotableCopyOnWriteArray<Object> array = new SnapshotableCopyOnWriteArray<>();
        array.addAll(Arrays.asList("foo", "bar"));
        Assertions.assertThat(array.size()).isEqualTo(2);

        List<Object> snapshot = array.snapshot();
        array.addAll(Arrays.asList("foo", "bar"));
        Assertions.assertThat(array.size()).isEqualTo(4);
        Assertions.assertThat(snapshot).containsExactly("foo", "bar");
    }

    @Test
    public void should_remove() {
        SnapshotableCopyOnWriteArray<Object> array = new SnapshotableCopyOnWriteArray<>();
        array.addAll(Arrays.asList("foo", "bar", "baz"));

        array.remove("bar");

        Assertions.assertThat(array.size()).isEqualTo(2);
        Assertions.assertThat(array.snapshot()).containsExactly("foo", "baz");
    }

    @Test
    public void should_remove_at_the_end() {
        SnapshotableCopyOnWriteArray<Object> array = new SnapshotableCopyOnWriteArray<>();
        array.addAll(Arrays.asList("foo", "bar", "baz"));

        array.remove("baz");

        Assertions.assertThat(array.size()).isEqualTo(2);
        Assertions.assertThat(array.snapshot()).containsExactly("foo", "bar");
    }

    @Test
    public void should_remove_at_the_beginning() {
        SnapshotableCopyOnWriteArray<Object> array = new SnapshotableCopyOnWriteArray<>();
        array.addAll(Arrays.asList("foo", "bar", "baz"));

        array.remove("bar");

        Assertions.assertThat(array.size()).isEqualTo(2);
        Assertions.assertThat(array.snapshot()).containsExactly("foo", "baz");
    }

    @Test
    public void should_not_remove_if_not_present() {
        SnapshotableCopyOnWriteArray<Object> array = new SnapshotableCopyOnWriteArray<>();
        array.addAll(Arrays.asList("foo", "bar"));

        array.remove("hello");

        Assertions.assertThat(array.size()).isEqualTo(2);
        Assertions.assertThat(array.snapshot()).containsExactly("foo", "bar");
    }

}