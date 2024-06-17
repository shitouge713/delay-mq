package org.finalcola.delay.mq.client.rocket;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collection;

/**
 * @author: finalcola
 * @date: 2023/3/27 22:27
 */
public interface RocketDelayProducer {

    SendResult sendDelayMessage(Collection<Message> msgs, @Nonnull Duration delayDuration) throws MQBrokerException, RemotingException, InterruptedException, MQClientException;
}
