package org.finalcola.delay.mq.broker.db.config;

import lombok.extern.slf4j.Slf4j;
import org.finalcola.dalay.mq.common.constants.MqType;
import org.finalcola.delay.mq.broker.config.BrokerConfig;
import org.finalcola.delay.mq.broker.config.MqConfig;
import org.finalcola.delay.mq.broker.config.PropertyConfigFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: finalcola
 * @date: 2023/4/1 11:58
 */
@Slf4j
public class PropertyConfigFactoryTest {

    @Test
    public void readPropertyTest() {
        BrokerConfig config = PropertyConfigFactory.createConfig(BrokerConfig.class);
        log.info("config:{}", config);
        Assert.assertEquals(config.getScanMsgBatchSize().intValue(), 50);

        MqConfig mqConfig = PropertyConfigFactory.createConfig(MqConfig.class);
        log.info("mqConfig:{}", mqConfig);
        Assert.assertEquals(mqConfig.getMqType(), MqType.ROCKET_MQ);
    }
}
