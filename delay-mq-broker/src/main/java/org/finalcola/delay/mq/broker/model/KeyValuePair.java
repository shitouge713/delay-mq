package org.finalcola.delay.mq.broker.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.ByteBuffer;

/**
 * @author: finalcola
 * @date: 2023/3/15 22:44
 */
@Data
@AllArgsConstructor
public class KeyValuePair {
    private final ByteBuffer key;
    private final ByteBuffer value;

    public KeyValuePair(String key, String value) {
        this(ByteBuffer.wrap(key.getBytes()), ByteBuffer.wrap(value.getBytes()));
    }
}
