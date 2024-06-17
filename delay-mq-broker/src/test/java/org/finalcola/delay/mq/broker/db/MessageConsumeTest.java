package org.finalcola.delay.mq.broker.db;

import com.google.protobuf.ByteString;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.Message;
import org.finalcola.dalay.mq.common.constants.Constants;
import org.finalcola.dalay.mq.common.constants.MqType;
import org.finalcola.delay.mq.broker.MessageOutput;
import org.finalcola.delay.mq.broker.MetaHolder;
import org.finalcola.delay.mq.broker.Scanner;
import org.finalcola.delay.mq.broker.config.BrokerConfig;
import org.finalcola.delay.mq.broker.config.MqConfig;
import org.finalcola.delay.mq.broker.config.RocksDBConfig;
import org.finalcola.delay.mq.broker.convert.MsgConverter;
import org.finalcola.delay.mq.broker.model.KeyValuePair;
import org.finalcola.delay.mq.common.proto.DelayMsg;
import org.finalcola.delay.mq.common.proto.MsgDataWrapper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.finalcola.dalay.mq.common.utils.MoreFunctions.runCatching;

/**
 * @author: finalcola
 * @date: 2023/3/29 22:38
 */
@Slf4j
public class MessageConsumeTest {

    private static final String TOPIC_TEST = "topic_test";

    @Rule
    public TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    @SneakyThrows
    public void consumerTest() {
        log.info("consumerTest start");
        RocksDBConfig dbConfig = RocksDBConfig.builder()
                .path(temporaryFolder.newFolder("rocks").getPath())
                .partitionCount(1)
                .build();
        RocksDBStore rocksDBStore = new RocksDBStore(dbConfig);
        rocksDBStore.start();

        MqConfig mqConfig = MqConfig.builder()
                .mqType(MqType.ROCKET_MQ)
                .brokerAddr("localhost:9876")
                .sendRetryTimes(10)
                .build();
        Scanner scanner = new Scanner(BrokerConfig.builder()
                .scanMsgBatchSize(50)
                .build(), rocksDBStore);
        int msgCount = 100;
        CountDownLatch latch = new CountDownLatch(msgCount);
        MessageOutput messageOutput = new MessageOutput(0, new MetaHolder(rocksDBStore, MqType.ROCKET_MQ), mqConfig, scanner) {
            @Override
            protected void produceMessageHook(List<DelayMsg> delayMsgs) {
                String topic = delayMsgs.get(0).getMetaData().getTopic();
                log.info("produceMessage topic:{} count:{}", topic, delayMsgs.size());
                Assert.assertEquals(topic, TOPIC_TEST);
                delayMsgs.forEach(v -> latch.countDown());
            }
        };
        try {
            messageOutput.start();
            // mock数据
            makeDelayMsg(rocksDBStore, Duration.ofSeconds(1), msgCount);
            latch.await(10, TimeUnit.SECONDS);
        } finally {
            runCatching(messageOutput::stop);
            runCatching(rocksDBStore::stop);
        }

    }

    @SneakyThrows
    private int makeDelayMsg(RocksDBStore rocksDBStore, Duration delay, int msgCount) {
        AtomicInteger counter = new AtomicInteger(0);
        List<KeyValuePair> keyValuePairs = IntStream.range(0, msgCount).boxed()
                .map(i -> {
                    byte[] body = StringUtils.repeat("x", i + 1).getBytes(StandardCharsets.UTF_8);
                    return new Message(TOPIC_TEST, body);
                })
                .map(msg -> {
                    MsgDataWrapper dataWrapper = MsgDataWrapper.newBuilder()
                            .setMsgKey(StringUtils.trimToEmpty(msg.getKeys()))
                            .setCreateTime(System.currentTimeMillis())
                            .setDelayMills(delay.toMillis())
                            .setTopic(msg.getTopic())
                            .setTags(StringUtils.trimToEmpty(msg.getTags()))
                            .setData(ByteString.copyFrom(msg.getBody()))
                            .build();
                    return new Message(Constants.DELAY_MSG_TOPIC, dataWrapper.toByteArray());
                })
                .map(msg -> {
                    MsgDataWrapper dataWrapper = runCatching(() -> MsgDataWrapper.parseFrom(msg.getBody()));
                    return MsgConverter.buildDelayMsg(counter.incrementAndGet() + "", dataWrapper);
                })
                .map(delayMsg -> {
                    ByteBuffer key = MsgConverter.buildKey(delayMsg);
                    ByteBuffer value = ByteBuffer.wrap(delayMsg.toByteArray());
                    return new KeyValuePair(key, value);
                })
                .collect(Collectors.toList());
        rocksDBStore.put(0, keyValuePairs);
        return keyValuePairs.size();
    }
}
