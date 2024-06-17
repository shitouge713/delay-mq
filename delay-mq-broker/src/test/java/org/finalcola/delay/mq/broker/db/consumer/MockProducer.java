package org.finalcola.delay.mq.broker.db.consumer;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.finalcola.delay.mq.broker.config.MqConfig;
import org.finalcola.delay.mq.broker.producer.Producer;
import org.finalcola.delay.mq.common.proto.DelayMsg;
import org.finalcola.delay.mq.common.proto.MetaData;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: finalcola
 * @date: 2023/3/23 22:37
 */
@Slf4j
@AllArgsConstructor
public class MockProducer implements Producer {

    private final int expectedMsgCount;
    private final AtomicInteger msgCounter = new AtomicInteger();
    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void start(MqConfig mqConfig) {
        log.info("start mockConsumer");
    }

    @Override
    public boolean send(@Nonnull List<DelayMsg> delayMsgList) {
        int count = msgCounter.addAndGet(delayMsgList.size());
        log.info("send msg.current:{} ids:{}", count, delayMsgList.stream().map(DelayMsg::getMetaData).map(MetaData::getMsgId).collect(Collectors.toList()));
        if (count >= expectedMsgCount) {
            latch.countDown();
        }
        return true;
    }

    @Override
    public void stop() {
        log.info("stop mockConsumer");
    }

    @SneakyThrows
    public int await(Duration timeout) {
        latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        return msgCounter.get();
    }
}
