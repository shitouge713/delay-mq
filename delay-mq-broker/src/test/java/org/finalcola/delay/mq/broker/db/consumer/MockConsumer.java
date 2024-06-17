package org.finalcola.delay.mq.broker.db.consumer;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;
import org.finalcola.delay.mq.broker.config.MqConfig;
import org.finalcola.delay.mq.broker.consumer.Consumer;
import org.finalcola.delay.mq.common.proto.DelayMsg;
import org.finalcola.delay.mq.common.proto.MetaData;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author: finalcola
 * @date: 2023/3/21 22:02
 */
@Slf4j
public class MockConsumer implements Consumer {

    private static final AtomicIntegerFieldUpdater<MockConsumer> OFFSET_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(MockConsumer.class, "offset");
    private final int round = 10;
    @Getter
    private final int batchSize;
    @Getter
    private final List<DelayMsg> delayMsgList;
    private final long delayStartTime;
    private final long delayEndTime;
    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile int offset = 0;

    public MockConsumer(int msgCount, Range<Long> delayTimeRange) {
        this.batchSize = Math.max(1, msgCount / 20);
        this.delayStartTime = delayTimeRange.getMinimum();
        this.delayEndTime = delayTimeRange.getMaximum();
        this.delayMsgList = makeMsg(msgCount);
    }

    @Override
    public void start(MqConfig config) {
        log.info("start");
    }

    @Override
    public void stop() {
        log.info("stop");
    }

    @Override
    public List<DelayMsg> poll() {
        if (offset >= delayMsgList.size()) {
            return Collections.emptyList();
        }
        int start = offset;
        int end = Math.min(offset + batchSize, delayMsgList.size());
        List<DelayMsg> delayMsgs = delayMsgList.subList(start, end);
        log.info("produce msg count:{}", delayMsgs.size());
        return delayMsgs;
    }

    @Override
    public void commitOffset() {
        int newOffset = OFFSET_UPDATER.addAndGet(this, batchSize);
        log.info("newOffset:{}", newOffset);
        if (newOffset >= delayMsgList.size()) {
            latch.countDown();
        }
    }

    @SneakyThrows
    public void await() {
        latch.await();
    }

    private List<DelayMsg> makeMsg(int msgCount) {
        List<DelayMsg> delayMsgs = Lists.newArrayListWithCapacity(msgCount);
        String topicPrefix = "test-mock-";
        for (int i = 0; i < msgCount; i++) {
            String topic = topicPrefix + i % round;
            long now = System.currentTimeMillis();
            MetaData metaData = MetaData.newBuilder()
                    .setMsgId(String.valueOf(i))
                    .setMsgKey("key_" + i)
                    .setCreateTimestamp(now)
                    .setDelayMills(makeDelayTime() - now)
                    .setTopic(topic)
                    .build();

            String value = StringUtils.repeat("v", i * 10);
            DelayMsg delayMsg = DelayMsg.newBuilder()
                    .setMetaData(metaData)
                    .setBody(ByteString.copyFromUtf8(value))
                    .build();
            delayMsgs.add(delayMsg);
        }
        return delayMsgs;
    }

    private long makeDelayTime() {
        int random = ThreadLocalRandom.current().nextInt((int) (delayEndTime - delayStartTime));
        return delayStartTime + random;
    }

}
