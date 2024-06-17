package org.finalcola.delay.mq.client.rocket;

import com.google.protobuf.ByteString;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.finalcola.dalay.mq.common.constants.Constants;
import org.finalcola.delay.mq.common.proto.MsgDataWrapper;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.trimToEmpty;

/**
 * @author: finalcola
 * @date: 2023/3/27 22:41
 */
public class DelayMQProducer extends DefaultMQProducer implements RocketDelayProducer {

    public DelayMQProducer() {
    }

    public DelayMQProducer(final String producerGroup) {
        this(null, producerGroup, null);
    }

    public DelayMQProducer(final String namespace, final String producerGroup) {
        this(namespace, producerGroup, null);
    }

    public DelayMQProducer(final String producerGroup, RPCHook rpcHook) {
        this(null, producerGroup, rpcHook);
    }

    public DelayMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook) {
        super(namespace, producerGroup, rpcHook);
    }

    public DelayMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook, boolean enableMsgTrace, final String customizedTraceTopic) {
        super(namespace, producerGroup, rpcHook, enableMsgTrace, customizedTraceTopic);
    }

    @Override
    public SendResult sendDelayMessage(Collection<Message> msgs, @Nonnull Duration delayDuration) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        List<Message> warpMessages = warpMessage(msgs, delayDuration);
        return send(warpMessages);
    }

    private List<Message> warpMessage(Collection<Message> msgs, @Nonnull Duration delayDuration) {
        return CollectionUtils.emptyIfNull(msgs).stream()
                .map(msg -> {
                    MsgDataWrapper dataWrapper = MsgDataWrapper.newBuilder()
                            .setMsgKey(trimToEmpty(msg.getKeys()))
                            .setCreateTime(System.currentTimeMillis())
                            .setDelayMills(delayDuration.toMillis())
                            .setTopic(msg.getTopic())
                            .setTags(trimToEmpty(msg.getTags()))
                            .setData(ByteString.copyFrom(msg.getBody()))
                            .build();
                    return new Message(Constants.DELAY_MSG_TOPIC, dataWrapper.toByteArray());
                })
                .collect(Collectors.toList());
    }
}
