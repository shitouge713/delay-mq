package org.finalcola.delay.mq.broker.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.finalcola.dalay.mq.common.constants.Property;
import org.finalcola.dalay.mq.common.constants.PropertyMapping;

/**
 * @author: finalcola
 * @date: 2023/3/18 21:28
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@PropertyMapping("broker.properties")
public class BrokerConfig {

  @Property(desc = "批量扫描消息数量", defaultValue = "100")
  private Integer scanMsgBatchSize;
}
