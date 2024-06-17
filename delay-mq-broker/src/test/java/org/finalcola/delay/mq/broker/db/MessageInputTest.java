package org.finalcola.delay.mq.broker.db;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Range;
import org.finalcola.delay.mq.broker.MessageInput;
import org.finalcola.delay.mq.broker.Scanner;
import org.finalcola.delay.mq.broker.config.BrokerConfig;
import org.finalcola.delay.mq.broker.config.RocksDBConfig;
import org.finalcola.delay.mq.broker.convert.MsgConverter;
import org.finalcola.delay.mq.broker.db.consumer.MockConsumer;
import org.finalcola.delay.mq.broker.model.ScanResult;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * @author: finalcola
 * @date: 2023/3/21 22:47
 */
@Slf4j
public class MessageInputTest {

    @Rule
    public TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @SneakyThrows
    @Test
    public void runTest() {
        long startTime = System.currentTimeMillis();
        File rocksPath = temporaryFolder.newFolder("rocks");
        RocksDBConfig rocksDBConfig = RocksDBConfig.builder()
                .partitionCount(1)
                .path(rocksPath.getPath())
                .build();
        RocksDBStore store = new RocksDBStore(rocksDBConfig);
        store.start();
        int msgCount = 100;
        MockConsumer consumer = new MockConsumer(msgCount,
                Range.<Long>between(DateTime.now().getMillis(), DateTime.now().plusSeconds(10).getMillis()));
        MessageInput messageInput = new MessageInput(0, store, consumer);
        messageInput.start();

        consumer.await();

        BrokerConfig config = BrokerConfig.builder()
                .scanMsgBatchSize(msgCount)
                .build();
        Scanner scanner = new Scanner(config, store);
        long currentTimeMillis = System.currentTimeMillis();
        ScanResult scanResult = scanner.scan(0, String.valueOf(startTime), true, MsgConverter.toByteBuffer(String.valueOf(currentTimeMillis)));
        log.info("scan msg count:{},lastKey:{}", scanResult.getDelayMsgs().size(), scanResult.getLastMsgStoreKey());
        long count = consumer.getDelayMsgList().stream()
                .filter(item -> item.getMetaData().getCreateTimestamp() + item.getMetaData().getDelayMills() <= currentTimeMillis)
                .count();
        assert scanResult.getDelayMsgs().size() == count;

        store.stop();
    }

}
