package org.finalcola.delay.mq.broker;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.finalcola.dalay.mq.common.constants.MqType;
import org.finalcola.dalay.mq.common.utils.RetryUtils;
import org.finalcola.delay.mq.broker.config.ExecutorDef;
import org.finalcola.delay.mq.broker.config.MqConfig;
import org.finalcola.delay.mq.broker.consumer.Consumer;
import org.finalcola.delay.mq.broker.consumer.RocketConsumer;
import org.finalcola.delay.mq.broker.convert.MsgConverter;
import org.finalcola.delay.mq.broker.db.RocksDBStore;
import org.finalcola.delay.mq.broker.model.KeyValuePair;
import org.finalcola.delay.mq.common.proto.DelayMsg;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.finalcola.dalay.mq.common.utils.MoreFunctions.runCatching;

/**
 * @author: finalcola
 * @date: 2023/3/18 23:47
 */
@Slf4j
public class MessageInput implements Runnable {

    @Getter
    private final int partitionId;
    private final MqConfig mqConfig;
    private final RocksDBStore store;
    private volatile Consumer consumer;
    private volatile boolean isRunning = false;

    public MessageInput(int partitionId, MqConfig mqConfig, RocksDBStore store) {
        this.partitionId = partitionId;
        this.mqConfig = mqConfig;
        this.store = store;
        this.consumer = createConsumer(mqConfig);
    }

    @VisibleForTesting
    public MessageInput(int partitionId, RocksDBStore store, Consumer consumer) {
        this.partitionId = partitionId;
        this.store = store;
        this.mqConfig = null;
        this.consumer = consumer;
    }

    public synchronized void start() {
        consumer.start(mqConfig);
        isRunning = true;
        ExecutorDef.MSG_INPUT_EXECUTOR.submit(() -> {
            while (isRunning) {
                try {
                    this.run();
                } catch (Exception e) {
                    log.info("message input pull error", e);
                    runCatching(() -> TimeUnit.SECONDS.sleep(1));
                }
            }
        });
    }

    public synchronized void stop() {
        Consumer oldConsumer = this.consumer;
        oldConsumer.stop();
        this.consumer = null;
        this.isRunning = false;
    }

    @Override
    public void run() {
        Consumer consumer = this.consumer;
        if (!isRunning || consumer == null) {
            return;
        }
        List<DelayMsg> delayMsgs = consumer.poll();
        if (CollectionUtils.isEmpty(delayMsgs)) {
            return;
        }
        pollMessageHook(delayMsgs);
        List<KeyValuePair> keyValuePairs = delayMsgs.stream()
                .map(msg -> {
                    ByteBuffer key = MsgConverter.buildKey(msg);
                    ByteBuffer value = ByteBuffer.wrap(msg.toByteArray());
                    return new KeyValuePair(key, value);
                })
                .collect(Collectors.toList());
        log.info("write message to rocksDB, partition:{} size:{}", partitionId, keyValuePairs.size());
        RetryUtils.retry(10, () -> {
            store.put(partitionId, keyValuePairs);
            consumer.commitOffset();
        });
    }

    protected void pollMessageHook(List<DelayMsg> delayMsgs) {
        // do nothing
    }

    private Consumer createConsumer(MqConfig mqConfig) {
        MqType mqType = mqConfig.getMqType();
        assert mqType != null;
        switch (mqType) {
            case ROCKET_MQ:
                return new RocketConsumer();
            case KAFKA:
            default:
                throw new RuntimeException(String.format("mqType:%s not support", mqType.name()));
        }
    }
}
