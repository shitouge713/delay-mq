package org.finalcola.delay.mq.broker.consumer;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.finalcola.dalay.mq.common.constants.Constants;
import org.finalcola.delay.mq.broker.config.MqConfig;
import org.finalcola.delay.mq.broker.convert.MsgConverter;
import org.finalcola.delay.mq.common.proto.DelayMsg;
import org.finalcola.delay.mq.common.proto.MsgDataWrapper;

import java.util.Collections;
import java.util.List;

/**
 * @author: finalcola
 * @date: 2023/3/17 22:52
 */
public class RocketConsumer implements Consumer {

    private volatile boolean isRunning = false;
    private MqConfig mqConfig;
    private DefaultLitePullConsumer consumer;

    @Override
    @SneakyThrows
    public void start(MqConfig config) {
        this.mqConfig = config;
        this.consumer = createConsumer();
        this.consumer.start();
        this.isRunning = true;
    }

    @Override
    public void stop() {
        isRunning = false;
        consumer.shutdown();
    }

    @SneakyThrows
    @Override
    public List<DelayMsg> poll() {
        if (!isRunning) {
            return Collections.emptyList();
        }
        List<MessageExt> messageExts = consumer.poll();
        List<DelayMsg> delayMsgs = Lists.newArrayListWithCapacity(messageExts.size());
        for (MessageExt msg : messageExts) {
            MsgDataWrapper wrapper = MsgDataWrapper.parseFrom(msg.getBody());
            DelayMsg delayMsg = MsgConverter.buildDelayMsg(msg.getMsgId(), wrapper);
            delayMsgs.add(delayMsg);
        }
        return delayMsgs;
    }

    @Override
    public void commitOffset() {
        consumer.commitSync();
    }

    private DefaultLitePullConsumer createConsumer() throws MQClientException {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(Constants.CONSUMER_GROUP);
        consumer.subscribe(Constants.DELAY_MSG_TOPIC, "*");
        consumer.setPullBatchSize(mqConfig.getPullBatchSize());
        consumer.setAutoCommit(false); // 手动提交
        consumer.setNamesrvAddr(mqConfig.getBrokerAddr());
        return consumer;
    }
}
