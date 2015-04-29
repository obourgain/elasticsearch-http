package com.github.obourgain.elasticsearch.http.concurrent;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This list have similar properties than {@link CopyOnWriteArrayList} but allows to get a consistent snapshot of the collection.
 * It supports multiple writers through CAS and retry-loop, but this may be more costly than a lock if the mutation rate is high.
 * <p/>
 * This does not give any guarantee regarding mutability of contained objects.
 *
 * @param <E>
 */
public class SnapshotableCopyOnWriteArray<E> {

    private static final Object[] EMPTY = new Object[0];

    protected AtomicReference<Object[]> array = new AtomicReference<>(EMPTY);

    public SnapshotableCopyOnWriteArray() {
    }

    public SnapshotableCopyOnWriteArray(Object[] array) {
        this.array.set(array);
    }

    public SnapshotableCopyOnWriteArray(Collection<E> collection) {
        Object[] objects = collection.toArray();
        this.array.set(objects);
    }

    public int size() {
        return array.get().length;
    }

    public List<E> snapshot() {
        return (List<E>) Arrays.asList(array.get());
    }

    public void add(E e) {
        while (true) {
            Object[] elements = array.get();
            Object[] newElements = Arrays.copyOf(elements, elements.length + 1);
            newElements[elements.length] = e;
            if (array.compareAndSet(elements, newElements)) {
                return;
            }
        }
    }

    public void remove(Object o) {
        while (true) {
            Object[] elements = array.get();
            Object[] newElements = new Object[elements.length - 1]; // we expect the element to be present, so eagerly allocate a smaller array

            for (int i = 0; i < elements.length ; i++) {
                Object element = elements[i];
                // last element is special case as we it is either :
                // * the object to remove and then we don't need to copy it
                // * it is not and so this collection does not contains the object to remove so we don't need to install the new array
                if(i == elements.length - 1) {
                    if(Objects.equals(elements[elements.length - 1], o)) {
                        // skip copying this element and try to install the new array
                        break;
                    } else {
                        // this is not the droid you are looking for, just return
                        return;
                    }
                }

                if (Objects.equals(element, o)) {
                    // skip this element and copy the remaining elements
                    System.arraycopy(elements, i + 1, newElements, i, elements.length - i - 1);
                    break;
                } else {
                    // copy to new array
                    newElements[i] = element;
                }
            }

            if (array.compareAndSet(elements, newElements)) {
                return;
            }
        }
    }

    public void addAll(Collection<? extends E> c) {
        while (true) {
            Object[] elements = array.get();
            int currentLength = elements.length;
            Object[] newElements = Arrays.copyOf(elements, currentLength + c.size());

            int index = 0;
            for (E e : c) {
                newElements[currentLength + index] = e;
                index++;
            }
            if (array.compareAndSet(elements, newElements)) {
                return;
            }
        }
    }

    public void clear() {
        this.array.set(EMPTY);
    }

}
