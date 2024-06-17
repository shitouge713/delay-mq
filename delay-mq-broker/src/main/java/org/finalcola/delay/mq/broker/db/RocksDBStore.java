package org.finalcola.delay.mq.broker.db;

import lombok.Getter;
import org.finalcola.delay.mq.broker.config.RocksDBConfig;
import org.finalcola.delay.mq.broker.model.KeyValuePair;
import org.rocksdb.*;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author: finalcola
 * @date: 2023/3/14 23:07
 */
public class RocksDBStore {

    @Getter
    private final RocksDBConfig config;
    @Getter
    private volatile boolean isRunning;
    private RocksDB rocksDB;
    private DBOptions dbOptions;
    private List<ColumnFamilyDescriptor> cfDescriptors;
    private List<ColumnFamilyHandle> cfHandles;
    private ColumnFamilyOptions cfOptions;
    private Map<Integer, RocksDBAccess> partitionDataAccessMap;
    private Map<ColumnFamilyType, RocksDBAccess> innerDataAccessMap;

    public RocksDBStore(@Nonnull RocksDBConfig config) {
        this.config = config;
    }

    public synchronized void start() throws RocksDBException {
        initDbOptions();
        initCfDescriptors();
        cfHandles = new ArrayList<>();
        rocksDB = RocksDB.open(dbOptions, config.getPath(), cfDescriptors, cfHandles);

        // 内部数据
        innerDataAccessMap = Arrays.stream(ColumnFamilyType.values())
                .collect(Collectors.toMap(Function.identity(), type -> new RocksDBAccess(rocksDB, cfHandles.get(type.ordinal()))));

        // 分区数据
        partitionDataAccessMap = IntStream.range(0, config.getPartitionCount()).boxed()
                .collect(Collectors.toMap(Function.identity(), index ->
                        new RocksDBAccess(rocksDB, cfHandles.get(index + ColumnFamilyType.values().length))));

        isRunning = true;
    }

    public synchronized void stop() {
        isRunning = false;
        cfHandles.forEach(AbstractImmutableNativeReference::close);
        rocksDB.close();
    }

    public void put(int partitionId, @Nonnull List<KeyValuePair> keyValuePairs) throws RocksDBException {
        partitionDataAccessMap.get(partitionId).batchPut(keyValuePairs);
    }

    public void deleteRange(int partitionId, @Nonnull ByteBuffer start, @Nonnull ByteBuffer end) throws RocksDBException {
        partitionDataAccessMap.get(partitionId).deleteRange(start, end);
    }

    public RocksDBRangeIterator range(int partitionId, @Nonnull ByteBuffer start, @Nonnull ByteBuffer end) {
        return partitionDataAccessMap.get(partitionId).range(start, end);
    }

    public ByteBuffer getInnerData(@Nonnull ColumnFamilyType type, @Nonnull ByteBuffer key) throws RocksDBException {
        return innerDataAccessMap.get(type).get(key);
    }

    public void putInnerData(@Nonnull ColumnFamilyType type, @Nonnull ByteBuffer key, @Nonnull ByteBuffer value) throws RocksDBException {
        innerDataAccessMap.get(type).put(new KeyValuePair(key, value));
    }

    private void initCfDescriptors() {
        // 初始化cf配置
        initCfOptions();
        int partitionCount = config.getPartitionCount();
        // 存放元数据的cf
        List<ColumnFamilyDescriptor> descriptors = Arrays.stream(ColumnFamilyType.values())
                .map(ColumnFamilyType::getName)
                .map(name -> new ColumnFamilyDescriptor(name.getBytes(UTF_8), cfOptions))
                .collect(Collectors.toList());
        // 数据分区，每个分区对应一个表
        IntStream.range(0, partitionCount)
                .mapToObj(index -> {
                    String cfName = "msg_partition_" + index;
                    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
                    return new ColumnFamilyDescriptor(cfName.getBytes(UTF_8), cfOptions);
                })
                .forEach(descriptors::add);
        cfDescriptors = descriptors;
    }

    private void initCfOptions() {
        cfOptions = new ColumnFamilyOptions();
    }

    private void initDbOptions() {
        dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);
    }
}