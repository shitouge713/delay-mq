package org.finalcola.delay.mq.broker.consumer;

import org.finalcola.delay.mq.broker.config.MqConfig;
import org.finalcola.delay.mq.common.proto.DelayMsg;

import java.util.List;

/**
 * @author: finalcola
 * @date: 2023/3/15 23:36
 */
public interface Consumer {

    void start(MqConfig config);

    void stop();

    List<DelayMsg> poll();

    void commitOffset();
}
