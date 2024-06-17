package org.finalcola.delay.mq.broker.db;

import com.google.protobuf.ByteString;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.finalcola.delay.mq.broker.MessageOutput;
import org.finalcola.delay.mq.broker.MetaHolder;
import org.finalcola.delay.mq.broker.Scanner;
import org.finalcola.delay.mq.broker.config.BrokerConfig;
import org.finalcola.delay.mq.broker.config.RocksDBConfig;
import org.finalcola.delay.mq.broker.convert.MsgConverter;
import org.finalcola.delay.mq.broker.db.consumer.MockProducer;
import org.finalcola.delay.mq.broker.db.model.TimeRange;
import org.finalcola.delay.mq.broker.model.KeyValuePair;
import org.finalcola.delay.mq.common.proto.MsgDataWrapper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.finalcola.dalay.mq.common.constants.MqType.MOCK;

/**
 * @author: finalcola
 * @date: 2023/3/23 22:35
 */
@Slf4j
public class MessageOutputTest {

    @Rule
    public TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    @SneakyThrows
    public void runTest() {
        RocksDBConfig rocksDBConfig = RocksDBConfig.builder()
                .path(temporaryFolder.newFolder("rocks").getPath())
                .partitionCount(1)
                .build();
        RocksDBStore rocksDBStore = new RocksDBStore(rocksDBConfig);
        rocksDBStore.start();
        int msgCount = 100;
        int maxWaitSeconds = 5;
        List<KeyValuePair> mockData = makeTestData(msgCount, TimeRange.withinSeconds(maxWaitSeconds).minus(Duration.ofSeconds(3)));
        Assert.assertEquals(mockData.size(), msgCount);
        rocksDBStore.put(0, mockData);

        BrokerConfig brokerConfig = BrokerConfig.builder().scanMsgBatchSize(10).build();
        MockProducer producer = new MockProducer(msgCount);
        Scanner scanner = new Scanner(brokerConfig, rocksDBStore);
        MessageOutput output = new MessageOutput(0, new MetaHolder(rocksDBStore, MOCK), producer, scanner);
        output.start();

        try {
            int producedCount = producer.await(Duration.ofSeconds(maxWaitSeconds));
            Assert.assertEquals(producedCount, msgCount);
        } finally {
            output.stop();
            rocksDBStore.stop();
        }
    }

    private List<KeyValuePair> makeTestData(int count, TimeRange timeRange) {
        long now = System.currentTimeMillis();
        int bound = (int) (timeRange.getEndTime() - timeRange.getStartTime());
        return IntStream.range(0, count)
                .mapToObj(index -> {
                    int delayMills = bound - index;
                    MsgDataWrapper wrapper = MsgDataWrapper.newBuilder()
                            .setMsgKey("key_" + index)
                            .setCreateTime(now)
                            .setDelayMills(delayMills)
                            .setTopic("test-topic")
                            .setTags("tags")
                            .setData(ByteString.copyFromUtf8(StringUtils.repeat("asd", 10)))
                            .build();
                    return MsgConverter.buildDelayMsg(index + "", wrapper);
                })
                .map(delayMsg -> new KeyValuePair(MsgConverter.buildKey(delayMsg), ByteBuffer.wrap(delayMsg.toByteArray())))
                .collect(Collectors.toList());
    }

}
