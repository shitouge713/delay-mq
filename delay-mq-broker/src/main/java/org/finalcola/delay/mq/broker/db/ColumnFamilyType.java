package org.finalcola.delay.mq.broker.db;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author: finalcola
 * @date: 2023/3/15 22:28
 */
@Getter
@AllArgsConstructor
public enum ColumnFamilyType {
    DEFAULT("default"),
    META_DATA("meta_data"),
    ;
    private final String name;
}
