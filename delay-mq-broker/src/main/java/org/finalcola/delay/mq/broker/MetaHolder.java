package org.finalcola.delay.mq.broker;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.finalcola.dalay.mq.common.constants.Constants;
import org.finalcola.dalay.mq.common.constants.MqType;
import org.finalcola.delay.mq.broker.config.ExecutorDef;
import org.finalcola.delay.mq.broker.convert.MsgConverter;
import org.finalcola.delay.mq.broker.db.ColumnFamilyType;
import org.finalcola.delay.mq.broker.db.RocksDBStore;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.finalcola.delay.mq.broker.convert.MsgConverter.toByteBuffer;

/**
 * @author: finalcola
 * @date: 2023/3/18 13:14
 */
@Slf4j
public class MetaHolder {
    private static final AtomicReferenceFieldUpdater<MetaHolder, String> LAST_HANDLE_TIME_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(MetaHolder.class, String.class, "lastHandledKey");
    private final RocksDBStore store;
    private final MqType mqType;
    private volatile String lastHandledKey = null;

    public MetaHolder(RocksDBStore store, MqType mqType) {
        this.store = store;
        this.mqType = mqType;
    }

    public synchronized void start() throws RocksDBException {
        ByteBuffer data = store.getInnerData(ColumnFamilyType.META_DATA, getStoreKey());
        lastHandledKey = Optional.ofNullable(data)
                .map(MsgConverter::toString)
                .orElseGet(() -> StringUtils.repeat("0", 13));
        ExecutorDef.SCHEDULER.scheduleAtFixedRate(() -> {
            try {
                save();
            } catch (RocksDBException e) {
                log.error("auto save meta data({}) error.", lastHandledKey, e);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public synchronized void stop() throws RocksDBException {
        save();
    }

    public void save() throws RocksDBException {
        ByteBuffer value = toByteBuffer(lastHandledKey);
        store.putInnerData(ColumnFamilyType.META_DATA, getStoreKey(), value);
    }

    public String getLastHandledKey() {
        return lastHandledKey;
    }

    public void setLastHandledKey(String lastHandledKey) {
        LAST_HANDLE_TIME_UPDATER.set(this, lastHandledKey);
    }

    public boolean setLastHandledKey(String oldVal, String newVal) {
        return LAST_HANDLE_TIME_UPDATER.compareAndSet(this, oldVal, newVal);
    }

    private ByteBuffer getStoreKey() {
        return toByteBuffer(Constants.META_DATA_STORE_KEY + "-" + mqType.name());
    }
}
