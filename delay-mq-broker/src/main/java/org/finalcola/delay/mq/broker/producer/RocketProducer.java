package org.finalcola.delay.mq.broker.producer;

import lombok.SneakyThrows;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.finalcola.dalay.mq.common.constants.Constants;
import org.finalcola.delay.mq.broker.config.MqConfig;
import org.finalcola.delay.mq.common.proto.DelayMsg;
import org.finalcola.delay.mq.common.proto.MetaData;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: finalcola
 * @date: 2023/3/18 21:47
 */
public class RocketProducer implements Producer {

    private DefaultMQProducer producer;
    private volatile boolean isRunning = false;

    @Override
    @SneakyThrows
    public void start(MqConfig mqConfig) {
        producer = new DefaultMQProducer(Constants.PRODUCER_GROUP);
        producer.setNamesrvAddr(mqConfig.getBrokerAddr());
        producer.setRetryTimesWhenSendFailed(mqConfig.getSendRetryTimes());
        producer.start();
        isRunning = true;
    }

    @SneakyThrows
    @Override
    public boolean send(@Nonnull List<DelayMsg> delayMsgList) {
        final DefaultMQProducer mqProducer = this.producer;
        if (!isRunning) {
            return false;
        }
        final List<Message> messageList = delayMsgList.stream()
                .map(delayMsg -> {
                    final MetaData metaData = delayMsg.getMetaData();
                    return new Message(metaData.getTopic(), metaData.getTags(), metaData.getMsgKey(), delayMsg.getBody().toByteArray());
                })
                .collect(Collectors.toList());
        final SendResult sendResult = mqProducer.send(messageList);
        return Optional.ofNullable(sendResult)
                .map(SendResult::getSendStatus)
                .filter(SendStatus.SEND_OK::equals)
                .isPresent();
    }

    @Override
    public void stop() {
        producer.shutdown();
        producer = null;
        isRunning = false;
    }
}
