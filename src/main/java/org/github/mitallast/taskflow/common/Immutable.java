package org.github.mitallast.taskflow.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;

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

    public static ImmutableMap<String, String> toMap(Config config) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
            switch (entry.getValue().valueType()) {
                case BOOLEAN:
                case NUMBER:
                case STRING:
                    String value = entry.getValue().unwrapped().toString();
                    builder.put(entry.getKey(), value);
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected entry type");
            }
        }
        return builder.build();
    }

    public static ImmutableMultimap<String, String> toMultimap(Config config) {
        ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
        for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
            switch (entry.getValue().valueType()) {
                case BOOLEAN:
                case NUMBER:
                case STRING:
                    String value = entry.getValue().unwrapped().toString();
                    builder.put(entry.getKey(), value);
                    break;
                case LIST:
                    ConfigList list = ((ConfigList) entry.getValue());
                    for (ConfigValue configValue : list) {
                        switch (configValue.valueType()) {
                            case BOOLEAN:
                            case NUMBER:
                            case STRING:
                                String listValue = entry.getValue().unwrapped().toString();
                                builder.put(entry.getKey(), listValue);
                                break;
                            default:
                                throw new IllegalArgumentException("Unexpected entry type");
                        }
                    }

                default:
                    throw new IllegalArgumentException("Unexpected entry type");
            }
        }
        return builder.build();
    }
}
