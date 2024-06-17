package org.finalcola.delay.mq.broker.db;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.Message;
import org.finalcola.dalay.mq.common.constants.MqType;
import org.finalcola.dalay.mq.common.utils.MoreFunctions;
import org.finalcola.delay.mq.broker.MessageInput;
import org.finalcola.delay.mq.broker.config.MqConfig;
import org.finalcola.delay.mq.broker.config.RocksDBConfig;
import org.finalcola.delay.mq.client.rocket.DelayMQProducer;
import org.finalcola.delay.mq.common.proto.DelayMsg;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author: finalcola
 * @date: 2023/3/28 22:23
 */
public class MessageProduceTest {

    private final String nameServerAddr = "192.168.8.3:9876";
    @Rule
    public TemporaryFolder temporaryFolder = TemporaryFolder.builder()
            .assureDeletion()
            .build();

    @Test
    @SneakyThrows
    public void sendDelayMsgTest() {
        // 启动MessageInput
        RocksDBStore store = new RocksDBStore(RocksDBConfig.builder()
                .partitionCount(1)
                .path(temporaryFolder.newFolder("rocks").getPath())
                .build());
        store.start();
        MqConfig mqConfig = MqConfig.builder()
                .brokerAddr(nameServerAddr)
                .mqType(MqType.ROCKET_MQ)
                .pullBatchSize(100)
                .sendRetryTimes(10)
                .build();
        List<Message> messages = makeMsg();
        CountDownLatch countDownLatch = new CountDownLatch(messages.size());

        MessageInput messageInput = new MessageInput(0, mqConfig, store) {
            @Override
            protected void pollMessageHook(List<DelayMsg> delayMsgs) {
                for (int i = 0; i < delayMsgs.size(); i++) {
                    countDownLatch.countDown();
                }
            }
        };
        messageInput.start();

        DelayMQProducer delayMQProducer = buildMQProducer();
        try {
            delayMQProducer.start();
            for (int i = 0; i < 10; i++) {
                delayMQProducer.sendDelayMessage(messages, Duration.ofMinutes(1));
            }
            countDownLatch.await(10, TimeUnit.SECONDS);
        } finally {
            delayMQProducer.shutdown();
            MoreFunctions.runCatching(messageInput::stop);
            store.stop();
        }
    }

    private List<Message> makeMsg() {
        return IntStream.range(0, 200)
                .boxed()
                .map(i -> {
                    String key = "key_" + i;
                    byte[] value = StringUtils.repeat(key, i).getBytes(StandardCharsets.UTF_8);
                    return new Message("topic_test", "tag", key, value);
                })
                .collect(Collectors.toList());
    }

    @SneakyThrows
    private DelayMQProducer buildMQProducer() {
        DelayMQProducer producer = new DelayMQProducer("test-p");
        producer.setNamesrvAddr(nameServerAddr);
        return producer;
    }
}
