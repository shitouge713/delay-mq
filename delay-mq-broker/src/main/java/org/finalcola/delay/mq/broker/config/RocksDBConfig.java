package org.finalcola.delay.mq.broker.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: finalcola
 * @date: 2023/3/14 23:56
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RocksDBConfig {
    private String path; // 存储地址
    private int partitionCount; // 分区数量
}
