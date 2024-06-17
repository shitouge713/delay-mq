package org.finalcola.delay.mq.broker.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.finalcola.dalay.mq.common.constants.MqType;
import org.finalcola.dalay.mq.common.constants.Property;
import org.finalcola.dalay.mq.common.constants.PropertyMapping;

/**
 * @author: finalcola
 * @date: 2023/3/17 23:27
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@PropertyMapping("mq.properties")
public class MqConfig {
    @Property(desc = "broker地址", required = true)
    private String brokerAddr;
    @Property(desc = "每次pull的消息数量", defaultValue = "100")
    private Integer pullBatchSize;
    @Property(desc = "发送消息的重试次数", defaultValue = "10")
    private Integer sendRetryTimes;
    @Property(desc = "消息队列类型", required = true)
    private MqType mqType;

}
