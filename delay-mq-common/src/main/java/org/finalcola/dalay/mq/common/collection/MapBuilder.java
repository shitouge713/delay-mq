package org.finalcola.dalay.mq.common.collection;

import org.apache.commons.collections4.MapUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: finalcola
 * @date: 2023/3/30 23:24
 */
public class MapBuilder<K, V> {

    private final Map<K, V> innerMap = new HashMap<>();

    public static <K, V> MapBuilder<K, V> newBuilder() {
        return new MapBuilder<>();
    }

    public MapBuilder<K, V> put(K k, V v) {
        innerMap.put(k, v);
        return this;
    }

    public MapBuilder<K, V> putAll(Map<K, V> other) {
        innerMap.putAll(MapUtils.emptyIfNull(other));
        return this;
    }

    public Map<K, V> build() {
        return new HashMap<>(innerMap);
    }
}
