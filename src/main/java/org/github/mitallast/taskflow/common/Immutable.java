package org.github.mitallast.taskflow.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

public final class Immutable {
    private Immutable() {
    }

    public static <T> Optional<T> headOpt(List<T> values) {
        if (values.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(values.get(0));
        }
    }

    public static <K, V> ImmutableListMultimap<K, V> groupList(Collection<V> values, Function<V, K> mapper) {
        ImmutableListMultimap.Builder<K, V> builder = ImmutableListMultimap.builder();
        for (V value : values) {
            builder.put(mapper.apply(value), value);
        }
        return builder.build();
    }

    public static <K, V extends Comparable<V>> ImmutableListMultimap<K, V> groupSorted(Collection<V> values, Function<V, K> mapper) {
        ImmutableListMultimap.Builder<K, V> builder = ImmutableListMultimap.builder();
        builder.orderValuesBy(Comparator.naturalOrder());
        for (V value : values) {
            builder.put(mapper.apply(value), value);
        }
        return builder.build();
    }

    public static <K, V> ImmutableMap<K, V> group(Collection<V> values, Function<V, K> mapper) {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (V value : values) {
            builder.put(mapper.apply(value), value);
        }
        return builder.build();
    }

    public static <K, V> ImmutableMap<K, V> reduce(ImmutableListMultimap<K, V> multimap, Function<ImmutableCollection<V>, V> reducer) {
        ImmutableMap.Builder<K, V> builder = new ImmutableMap.Builder<>();
        for (K key : multimap.keySet()) {
            builder.put(key, reducer.apply(multimap.get(key)));
        }
        return builder.build();
    }

    public static <K, V, T> ImmutableListMultimap<K, T> map(ImmutableListMultimap<K, V> multimap, Function<ImmutableCollection<V>, Iterable<T>> mapper) {
        ImmutableListMultimap.Builder<K, T> builder = ImmutableListMultimap.builder();
        for (K key : multimap.keySet()) {
            builder.putAll(key, mapper.apply(multimap.get(key)));
        }
        return builder.build();
    }

    public static <V, T> ImmutableList<T> map(Collection<V> list, Function<V, T> mapper) {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        for (V v : list) {
            builder.add(mapper.apply(v));
        }
        return builder.build();
    }

    public static <V, T> ImmutableList<T> flatMap(Collection<V> list, Function<V, Iterable<T>> mapper) {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        for (V v : list) {
            builder.addAll(mapper.apply(v));
        }
        return builder.build();
    }

    public static <T> ImmutableList<T> sort(Collection<T> values, Comparator<? super T> comparator) {
        ArrayList<T> arrayList = new ArrayList<>(values);
        arrayList.sort(comparator);
        return ImmutableList.copyOf(arrayList);
    }

    public static <T extends Comparable<T>> ImmutableList<T> sort(Collection<T> values) {
        return sort(values, Comparator.naturalOrder());
    }

    public static <T extends Comparable<T>> ImmutableList<T> reverseSort(Collection<T> values) {
        return sort(values, Comparator.reverseOrder());
    }

    public static <T> T last(ImmutableCollection<T> values) {
        Preconditions.checkArgument(!values.isEmpty(), "Cannot be empty");
        return values.asList().get(values.size() - 1);
    }

    public static <T> ImmutableList<T> replace(ImmutableList<T> list, Predicate<T> match, T value) {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        for (T t : list) {
            if (match.test(t)) {
                builder.add(value);
            } else {
                builder.add(t);
            }
        }
        return builder.build();
    }

    public static <T> ImmutableList<T> append(ImmutableList<T> list, T... values) {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        builder.addAll(list).add(values);
        return builder.build();
    }
}
