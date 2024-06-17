package org.finalcola.delay.mq.broker.db;

import lombok.extern.slf4j.Slf4j;
import org.finalcola.delay.mq.broker.config.RocksDBConfig;
import org.finalcola.delay.mq.broker.model.KeyValuePair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * @author: shanshan
 * @date: 2023/3/15 23:01
 */
@Slf4j
public class RocksDBStoreTest {

    private RocksDBStore store;

    @Before
    public void before() throws RocksDBException {
        store = new RocksDBStore(createDBConfig());
        store.start();
    }

    @After
    public void stop() {
        store.stop();
    }

    @Test
    public void putAndRange() throws RocksDBException, IOException {
        long now = System.currentTimeMillis();
        Function<Integer, Stream<String>> keyFormatter = i -> {
            String keyPrefix = String.valueOf(now + i);
            return Stream.of(keyPrefix + "_aaa", keyPrefix + "_bbb");
        };

        List<KeyValuePair> pairList = IntStream.range(0, 10)
                .boxed()
                .flatMap(i -> {
                    return keyFormatter.apply(i)
                            .peek(k -> log.info("key:{}", k))
                            .map(k -> new KeyValuePair(k, String.valueOf(i)));
                })
                .collect(Collectors.toList());
        store.put(0, pairList);

        ByteBuffer start = wrap(keyFormatter.apply(5).findFirst().get());
        ByteBuffer end = wrap(keyFormatter.apply(10).findFirst().get());
        try (RocksDBRangeIterator iterator = store.range(0, start, end)) {
            iterator.forEachRemaining(pair -> {
                String key = new String(pair.getKey().array());
                String value = new String(pair.getValue().array());
//                System.out.printf("%s -> %s%n", key, value);
                log.info("k:{} v:{}", key, value);
            });
        }
    }

    @Test
    public void put() {
    }

    @Test
    public void range() {
    }

    private ByteBuffer wrap(String str) {
        return ByteBuffer.wrap(str.getBytes());
    }

    private RocksDBConfig createDBConfig() {
        return RocksDBConfig.builder()
                .partitionCount(10)
                .path("D:\\workspace\\runenv\\delay-server\\rocksdb")
                .build();
    }
}