package org.finalcola.delay.mq.broker.db;

import lombok.AllArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.finalcola.delay.mq.broker.model.KeyValuePair;
import org.rocksdb.*;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author: finalcola
 * @date: 2023/3/15 22:30
 */
@AllArgsConstructor
public class RocksDBAccess {
    private final RocksDB rocksDB;
    private final ColumnFamilyHandle handle;
    private final WriteOptions writeOptions = new WriteOptions()
            .setSync(false);

    public void put(@Nonnull KeyValuePair pair) throws RocksDBException {
        rocksDB.put(handle, writeOptions, pair.getKey().array(), pair.getValue().array());
    }

    public void batchPut(@Nonnull List<KeyValuePair> keyValuePairs) throws RocksDBException {
        if (CollectionUtils.isEmpty(keyValuePairs)) {
            return;
        }
        WriteBatch writeBatch = new WriteBatch();
        for (KeyValuePair pair : keyValuePairs) {
            writeBatch.put(handle, pair.getKey().array(), pair.getValue().array());
        }
        rocksDB.write(writeOptions, writeBatch);
    }

    public RocksDBRangeIterator range(@Nonnull ByteBuffer start, @Nonnull ByteBuffer end) {
        RocksIterator iterator = rocksDB.newIterator(handle);
        return new RocksDBRangeIterator(iterator, start, end);
    }

    public void deleteRange(@Nonnull ByteBuffer start, @Nonnull ByteBuffer end) throws RocksDBException {
        rocksDB.deleteRange(handle, writeOptions, start.array(), end.array());
    }

    public ByteBuffer get(@Nonnull ByteBuffer key) throws RocksDBException {
        return ByteBuffer.wrap(rocksDB.get(handle, key.array()));
    }
}
