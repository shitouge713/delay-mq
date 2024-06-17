package org.finalcola.delay.mq.broker.db;

import org.finalcola.delay.mq.broker.model.KeyValuePair;
import org.rocksdb.RocksIterator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * @author: finalcola
 * @date: 2023/3/15 22:48
 */
public class RocksDBRangeIterator implements Iterator<KeyValuePair>, Closeable {
    private final RocksIterator rocksIterator;
    private final ByteBuffer start;
    private final ByteBuffer end;
    private KeyValuePair next;

    public RocksDBRangeIterator(RocksIterator rocksIterator, ByteBuffer start, ByteBuffer end) {
        this.rocksIterator = rocksIterator;
        this.start = start;
        this.end = end;
        rocksIterator.seek(start);
    }

    @Override
    public boolean hasNext() {
        KeyValuePair makeResult = makeNext();
        this.next = makeResult;
        return makeResult != null;
    }

    @Override
    public KeyValuePair next() {
        try {
            return next;
        } finally {
            next = null;
        }
    }

    private KeyValuePair makeNext() {
        if (!rocksIterator.isValid()) {
            return null;
        }
        byte[] key = rocksIterator.key();
        if (key == null || key.length <= 0) {
            return null;
        }
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        if (keyBuffer.compareTo(end) > 0) {
            return null;
        }
        ByteBuffer value = ByteBuffer.wrap(rocksIterator.value());
        rocksIterator.next();
        return new KeyValuePair(keyBuffer, value);
    }

    @Override
    public void close() throws IOException {
        next = null;
        rocksIterator.close();
    }
}
