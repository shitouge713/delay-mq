package org.finalcola.delay.mq.broker.producer;

import org.finalcola.delay.mq.broker.config.MqConfig;
import org.finalcola.delay.mq.common.proto.DelayMsg;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * @author: finalcola
 * @date: 2023/3/18 21:47
 */
public interface Producer {

    void start(MqConfig mqConfig);

    boolean send(@Nonnull List<DelayMsg> delayMsgList);

    void stop();
}
